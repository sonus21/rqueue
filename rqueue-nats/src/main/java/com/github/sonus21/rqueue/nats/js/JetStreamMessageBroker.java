/*
 * Copyright (c) 2026 Sonu Kumar
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * You may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 */
package com.github.sonus21.rqueue.nats.js;

import static java.nio.charset.StandardCharsets.UTF_8;

import com.github.sonus21.rqueue.core.RqueueMessage;
import com.github.sonus21.rqueue.core.spi.Capabilities;
import com.github.sonus21.rqueue.core.spi.MessageBroker;
import com.github.sonus21.rqueue.enums.QueueType;
import com.github.sonus21.rqueue.listener.QueueDetail;
import com.github.sonus21.rqueue.nats.RqueueNatsConfig;
import com.github.sonus21.rqueue.nats.RqueueNatsException;
import com.github.sonus21.rqueue.nats.internal.NatsProvisioner;
import com.github.sonus21.rqueue.serdes.RqJacksonSerDes;
import com.github.sonus21.rqueue.serdes.RqueueSerDes;
import com.github.sonus21.rqueue.serdes.SerializationUtils;
import com.github.sonus21.rqueue.utils.PriorityUtils;
import io.nats.client.Connection;
import io.nats.client.Dispatcher;
import io.nats.client.JetStream;
import io.nats.client.JetStreamApiException;
import io.nats.client.JetStreamManagement;
import io.nats.client.JetStreamSubscription;
import io.nats.client.Message;
import io.nats.client.PullSubscribeOptions;
import io.nats.client.impl.Headers;
import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;
import java.util.logging.Level;
import java.util.logging.Logger;
import reactor.core.publisher.Mono;

/**
 * JetStream-backed implementation of {@link MessageBroker}.
 *
 * <p>This class keeps a per-instance in-memory map ({@code inFlight}) of NATS messages popped via
 * {@link #pop} so that {@link #ack} / {@link #nack} can locate the underlying NATS message handle.
 * The map is intentionally local: a process restart loses any pending entries, which is consistent
 * with the v1 capability set declaring no scheduled introspection. NATS itself will redeliver
 * unacked messages after {@code ackWait}.
 *
 * <p>Delayed enqueue and any scheduled/cron features throw {@link UnsupportedOperationException}.
 * {@code moveExpired} is a no-op returning 0; redelivery is handled by JetStream's ack-wait timer.
 */
public class JetStreamMessageBroker implements MessageBroker, AutoCloseable {

  private static final Logger log = Logger.getLogger(JetStreamMessageBroker.class.getName());

  /**
   * NATS message-scheduling header (ADR-51, NATS >= 2.12).
   * Value format: {@code @at <RFC3339-UTC>}, e.g. {@code @at 2026-05-09T12:30:00Z}.
   * The message must be published to a dedicated scheduler subject (not the work subject);
   * the work subject is declared via {@link #HDR_SCHEDULE_TARGET}.
   */
  public static final String HDR_SCHEDULE = "Nats-Schedule";

  /**
   * Target subject to which NATS fires the work message when the schedule triggers.
   * Must differ from the publish (scheduler) subject.
   */
  public static final String HDR_SCHEDULE_TARGET = "Nats-Schedule-Target";

  /**
   * Suffix appended to the work subject to form the per-message scheduler subject,
   * e.g. {@code rqueue.js.orders.sched.<msgId>}.
   * The stream must include {@code <workSubject>.sched.*} in its subjects list.
   */
  public static final String SCHED_SUBJECT_SUFFIX = ".sched.";

  /**
   * Enrichment header: epoch-ms at which this message was scheduled to be processed.
   * Written at scheduling publish time; read back in {@code enrichFromDelivery} so that
   * {@link RqueueMessage#getProcessAt()} can be populated without deserializing the payload.
   */
  static final String HDR_PROCESS_AT = "Rqueue-Process-At";

  /**
   * Enrichment header: period in milliseconds for periodic messages.
   * Written at scheduling publish time; read back in {@code enrichFromDelivery} to restore
   * {@link RqueueMessage#getPeriod()} for payloads that have it as zero.
   */
  static final String HDR_PERIOD = "Rqueue-Period";

  /**
   * Lower bound for fetch wait when the caller passes a non-positive duration. JetStream rejects
   * zero on a pull fetch, so any zero/negative wait is rounded up to this minimum. Callers that
   * want long-poll semantics should pass the desired wait explicitly (e.g. the listener
   * container's {@code pollingInterval}); this constant only guards against accidental zero waits
   * from non-listener callers.
   */
  private static final Duration MIN_FETCH_WAIT = Duration.ofMillis(50);

  /** RFC 3339 UTC formatter for the {@code Nats-Schedule} header value (@at prefix). */
  private static final DateTimeFormatter RFC3339_UTC =
      DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss'Z'").withZone(ZoneOffset.UTC);

  private final Connection connection;
  private final JetStream js;
  private final JetStreamManagement jsm;
  private final RqueueNatsConfig config;
  private final RqueueSerDes serdes;
  private final NatsProvisioner provisioner;
  private final boolean schedulingSupported;
  private final Capabilities caps;

  /**
   * keyed by {@code "<consumerName>::<RqueueMessage.id>"}, value is the underlying NATS
   * Message for ack/nak. The consumer prefix is required for Limits-retention streams where
   * multiple durable consumers each receive their own copy of every message — keying on
   * just the message id would let one consumer's {@code put} overwrite another's, and the
   * subsequent ack would target the wrong NATS Message handle (leaving the original delivery
   * stuck in {@code numAckPending} until {@code AckWait} expires).
   */
  private final ConcurrentHashMap<String, Message> inFlight = new ConcurrentHashMap<>();

  private static String inFlightKey(String consumerName, String messageId) {
    return (consumerName == null ? "" : consumerName) + "::" + messageId;
  }

  /**
   * Cached pull subscriptions keyed by stream + consumerName so we don't re-bind on every pop.
   */
  private final ConcurrentHashMap<String, JetStreamSubscription> subscriptionCache =
      new ConcurrentHashMap<>();

  // Public so tests in sibling packages (e.g. JetStreamMessageBrokerDelayThrowsTest) can build a
  // broker directly without going through builder() — keeps the regression caught by that test
  // pinned to the constructor signature itself.
  public JetStreamMessageBroker(
      Connection connection,
      JetStream js,
      JetStreamManagement jsm,
      RqueueNatsConfig config,
      RqueueSerDes serdes,
      NatsProvisioner provisioner) {
    this.connection = connection;
    this.js = js;
    this.jsm = jsm;
    this.config = config;
    this.serdes = serdes;
    this.provisioner = provisioner;
    this.schedulingSupported = provisioner != null && provisioner.isMessageSchedulingSupported();
    this.caps = new Capabilities(
        schedulingSupported, // supportsDelayedEnqueue  — requires NATS >= 2.12
        false, // supportsScheduledIntrospection — no inspectable scheduled-zset
        false, // supportsCronJobs         — no server-side cron
        false, // usesPrimaryHandlerDispatch — no Redis processing-ZSET
        true, // supportsViewData         — peek() reads from JetStream stream
        true); // supportsMoveMessage      — NatsRqueueUtilityService.moveMessage()
  }

  public static Builder builder() {
    return new Builder();
  }

  // ---- subject / stream naming -------------------------------------------

  private String subjectFor(QueueDetail q) {
    return config.getSubjectPrefix() + q.getName();
  }

  private String streamFor(QueueDetail q) {
    return config.getStreamPrefix() + q.getName();
  }

  /**
   * Resolve the priority-specific subject. Uses the same {@code "_priority"} suffix as
   * {@link com.github.sonus21.rqueue.utils.PriorityUtils#getSuffix(String)} so the subject
   * matches the expanded {@link QueueDetail#getName()} used by the poller (e.g. {@code "pq_high"}).
   */
  private String subjectFor(QueueDetail q, String priority) {
    if (priority == null || priority.isEmpty()) {
      return subjectFor(q);
    }
    return config.getSubjectPrefix() + q.getName() + PriorityUtils.getSuffix(priority);
  }

  /**
   * Resolve the priority-specific stream. Uses the same {@code "_priority"} suffix as
   * {@link com.github.sonus21.rqueue.utils.PriorityUtils#getSuffix(String)} so the stream name
   * matches what the poller derives from the expanded {@link QueueDetail#getName()}.
   */
  private String streamFor(QueueDetail q, String priority) {
    if (priority == null || priority.isEmpty()) {
      return streamFor(q);
    }
    return config.getStreamPrefix() + q.getName() + PriorityUtils.getSuffix(priority);
  }

  private String dlqStreamFor(QueueDetail q) {
    return streamFor(q) + config.getDlqStreamSuffix();
  }

  private String dlqSubjectFor(QueueDetail q) {
    return subjectFor(q) + config.getDlqSubjectSuffix();
  }

  /**
   * Scheduler subject for a single delayed message: {@code <workSubject>.sched.<msgId>}.
   * The NATS scheduler fires a publish to the work subject when the schedule triggers.
   * Each message gets its own unique subject so the rollup-sub only replaces itself,
   * never another message's schedule entry.
   */
  private String schedSubjectFor(QueueDetail q, String msgId) {
    return subjectFor(q) + SCHED_SUBJECT_SUFFIX + msgId;
  }

  /**
   * Wildcard pattern covering all scheduler subjects for a queue, used when registering
   * the stream's subject list. E.g. {@code rqueue.js.orders.sched.*}.
   */
  static String schedSubjectPattern(String workSubject) {
    return workSubject + SCHED_SUBJECT_SUFFIX + "*";
  }

  /** Stream description shown in {@code nats stream info} so operators can map back to rqueue. */
  private static String streamDescription(QueueDetail q) {
    return "rqueue queue: " + q.getName();
  }

  /** Stream description for the priority sub-stream. */
  private static String streamDescription(QueueDetail q, String priority) {
    return priority == null || priority.isEmpty()
        ? streamDescription(q)
        : "rqueue queue: " + q.getName() + " (priority=" + priority + ")";
  }

  private static String dlqStreamDescription(QueueDetail q) {
    return "rqueue DLQ for queue: " + q.getName();
  }

  // ---- MessageBroker -----------------------------------------------------

  @Override
  public void enqueue(QueueDetail q, RqueueMessage m) {
    String subject = subjectFor(q);
    String stream = streamFor(q);
    provisioner.ensureStream(stream, List.of(subject), q.getType(), streamDescription(q));
    Headers headers = new Headers();
    if (m.getId() != null) {
      headers.add("Nats-Msg-Id", m.getId());
    }
    try {
      byte[] payload = serdes.serialize(m);
      js.publish(subject, headers, payload);
    } catch (IOException | JetStreamApiException e) {
      throw new RqueueNatsException(
          "Failed to enqueue message id="
              + m.getId()
              + " queue="
              + q.getName()
              + " subject="
              + subject,
          e);
    } catch (RuntimeException e) {
      throw new RqueueNatsException(
          "Failed to serialize/enqueue message id="
              + m.getId()
              + " queue="
              + q.getName()
              + " subject="
              + subject,
          e);
    }
  }

  @Override
  public void enqueue(QueueDetail q, String priority, RqueueMessage m) {
    String subject = subjectFor(q, priority);
    String stream = streamFor(q, priority);
    provisioner.ensureStream(stream, List.of(subject), q.getType(), streamDescription(q, priority));
    Headers headers = new Headers();
    if (m.getId() != null) {
      headers.add("Nats-Msg-Id", m.getId());
    }
    try {
      byte[] payload = serdes.serialize(m);
      js.publish(subject, headers, payload);
    } catch (IOException | JetStreamApiException e) {
      throw new RqueueNatsException(
          "Failed to enqueue message id="
              + m.getId()
              + " queue="
              + q.getName()
              + " priority="
              + priority
              + " subject="
              + subject,
          e);
    } catch (RuntimeException e) {
      throw new RqueueNatsException(
          "Failed to serialize/enqueue message id="
              + m.getId()
              + " queue="
              + q.getName()
              + " priority="
              + priority
              + " subject="
              + subject,
          e);
    }
  }

  @Override
  public void enqueueWithDelay(QueueDetail q, RqueueMessage m, long delayMs) {
    if (!schedulingSupported) {
      throw new RqueueNatsException(
          "NATS message scheduling (ADR-51) is not available: the connected server is older than "
              + NatsProvisioner.SCHEDULING_MIN_VERSION
              + ". Upgrade NATS to "
              + NatsProvisioner.SCHEDULING_MIN_VERSION
              + "+ or use the Redis backend for delayed messages.");
    }
    String workSubject = subjectFor(q);
    String stream = streamFor(q);
    provisioner.ensureStream(
        stream,
        List.of(workSubject, schedSubjectPattern(workSubject)),
        q.getType(),
        streamDescription(q),
        true);
    String schedSubject = schedSubjectFor(q, m.getId());
    Headers headers = buildSchedulingHeaders(m, delayMs, workSubject);
    try {
      byte[] payload = serdes.serialize(m);
      js.publish(schedSubject, headers, payload);
    } catch (IOException | JetStreamApiException e) {
      throw new RqueueNatsException(
          "Failed to enqueue scheduled message id="
              + m.getId()
              + " queue="
              + q.getName()
              + " subject="
              + schedSubject,
          e);
    } catch (RuntimeException e) {
      throw new RqueueNatsException(
          "Failed to serialize/enqueue scheduled message id="
              + m.getId()
              + " queue="
              + q.getName()
              + " subject="
              + schedSubject,
          e);
    }
  }

  @Override
  public Mono<Void> enqueueReactive(QueueDetail q, RqueueMessage m) {
    String subject = subjectFor(q);
    String stream = streamFor(q);
    try {
      provisioner.ensureStream(stream, List.of(subject), q.getType(), streamDescription(q));
    } catch (Exception e) {
      return Mono.error(new RqueueNatsException(
          "Failed to provision stream for reactive enqueue id="
              + m.getId()
              + " queue="
              + q.getName(),
          e));
    }
    Headers headers = new Headers();
    if (m.getId() != null) {
      headers.add("Nats-Msg-Id", m.getId());
    }
    byte[] payload;
    try {
      payload = serdes.serialize(m);
    } catch (RuntimeException | IOException e) {
      return Mono.error(new RqueueNatsException(
          "Failed to serialize message id="
              + m.getId()
              + " queue="
              + q.getName()
              + " subject="
              + subject,
          e));
    }
    return Mono.fromFuture(() -> js.publishAsync(subject, headers, payload))
        .onErrorMap(e -> e instanceof RqueueNatsException
            ? e
            : new RqueueNatsException(
                "Failed to enqueue message id="
                    + m.getId()
                    + " queue="
                    + q.getName()
                    + " subject="
                    + subject,
                e))
        .then();
  }

  @Override
  public Mono<Void> enqueueWithDelayReactive(QueueDetail q, RqueueMessage m, long delayMs) {
    if (!schedulingSupported) {
      return Mono.error(new RqueueNatsException(
          "NATS message scheduling (ADR-51) is not available: the connected server is older than "
              + NatsProvisioner.SCHEDULING_MIN_VERSION
              + ". Upgrade NATS to "
              + NatsProvisioner.SCHEDULING_MIN_VERSION
              + "+ or use the Redis backend for delayed messages."));
    }
    String workSubject = subjectFor(q);
    String stream = streamFor(q);
    try {
      provisioner.ensureStream(
          stream,
          List.of(workSubject, schedSubjectPattern(workSubject)),
          q.getType(),
          streamDescription(q),
          true);
    } catch (Exception e) {
      return Mono.error(new RqueueNatsException(
          "Failed to provision stream for reactive scheduled enqueue id="
              + m.getId()
              + " queue="
              + q.getName(),
          e));
    }
    String schedSubject = schedSubjectFor(q, m.getId());
    Headers headers = buildSchedulingHeaders(m, delayMs, workSubject);
    byte[] payload;
    try {
      payload = serdes.serialize(m);
    } catch (RuntimeException | IOException e) {
      return Mono.error(new RqueueNatsException(
          "Failed to serialize scheduled message id="
              + m.getId()
              + " queue="
              + q.getName()
              + " subject="
              + schedSubject,
          e));
    }
    return Mono.fromFuture(() -> js.publishAsync(schedSubject, headers, payload))
        .onErrorMap(e -> e instanceof RqueueNatsException
            ? e
            : new RqueueNatsException(
                "Failed to enqueue scheduled message id="
                    + m.getId()
                    + " queue="
                    + q.getName()
                    + " subject="
                    + schedSubject,
                e))
        .then();
  }

  @Override
  public List<RqueueMessage> pop(QueueDetail q, String consumerName, int batch, Duration wait) {
    return popInternal(
        streamFor(q),
        subjectFor(q),
        resolveConsumerName(q.getName(), consumerName),
        batch,
        wait,
        resolveAckWait(q, config),
        resolveMaxDeliver(q, config));
  }

  @Override
  public List<RqueueMessage> pop(
      QueueDetail q, String priority, String consumerName, int batch, Duration wait) {
    return popInternal(
        streamFor(q, priority),
        subjectFor(q, priority),
        resolveConsumerName(q.getName(), consumerName),
        batch,
        wait,
        resolveAckWait(q, config),
        resolveMaxDeliver(q, config));
  }

  private static String resolveConsumerName(String queueName, String consumerName) {
    return (consumerName != null && !consumerName.isEmpty()) ? consumerName : "rqueue-" + queueName;
  }

  /**
   * Resolve the JetStream {@code ackWait} for this queue's pull consumer: per-queue
   * {@link QueueDetail#getVisibilityTimeout()} (when positive), else the global
   * {@code RqueueNatsConfig.ConsumerDefaults.getAckWait()}. Honouring visibilityTimeout makes
   * the NATS backend match the contract every other rqueue backend exposes: a message stays
   * invisible to other consumers for that window and is redelivered if not acked in time.
   */
  public static Duration resolveAckWait(QueueDetail q, RqueueNatsConfig config) {
    long vt = q.getVisibilityTimeout();
    if (vt > 0) {
      return Duration.ofMillis(vt);
    }
    return config.getConsumerDefaults().getAckWait();
  }

  /**
   * Resolve the JetStream {@code maxDeliver} from per-queue {@link QueueDetail#getNumRetry()}
   * (counted as initial delivery + N retries = numRetry + 1). The {@link Integer#MAX_VALUE}
   * "retry forever" sentinel maps to JetStream's unlimited value ({@code -1}); non-positive
   * numRetry falls back to {@code RqueueNatsConfig.ConsumerDefaults.getMaxDeliver()}.
   */
  public static long resolveMaxDeliver(QueueDetail q, RqueueNatsConfig config) {
    int numRetry = q.getNumRetry();
    if (numRetry == Integer.MAX_VALUE) {
      return -1L;
    }
    if (numRetry > 0) {
      return numRetry + 1L;
    }
    return config.getConsumerDefaults().getMaxDeliver();
  }

  private List<RqueueMessage> popInternal(
      String stream,
      String subject,
      String consumerName,
      int batch,
      Duration wait,
      Duration ackWait,
      long maxDeliver) {
    // Honour the caller-supplied wait — this is the listener container's pollingInterval for
    // RqueueMessagePoller, and lets JetStream long-poll instead of the broker firing a steady
    // stream of $JS.API.CONSUMER.MSG.NEXT requests. Only fall back when the caller didn't
    // express a preference; zero/negative waits are rounded up to the JetStream minimum.
    Duration fetchWait;
    if (wait == null) {
      fetchWait = config.getDefaultFetchWait();
    } else if (wait.isZero() || wait.isNegative()) {
      fetchWait = MIN_FETCH_WAIT;
    } else {
      fetchWait = wait;
    }
    String key = stream + "/" + consumerName;
    JetStreamSubscription sub = subscriptionCache.computeIfAbsent(key, k -> {
      // NatsStreamValidator provisions the stream and consumer at bootstrap (RqueueBootstrapEvent).
      // NatsProvisioner caches both, so ensureConsumer here is a map lookup — no backend call.
      try {
        String actualConsumerName = provisioner.ensureConsumer(
            stream,
            consumerName,
            subject, // filter: consumer only receives work-subject messages, not sched entries
            ackWait,
            maxDeliver,
            config.getConsumerDefaults().getMaxAckPending());
        PullSubscribeOptions opts = PullSubscribeOptions.bind(stream, actualConsumerName);
        // The filter is set on the consumer itself; pass null here so the NATS client does not
        // re-validate the subject against the consumer filter at subscribe time — SUB-90011
        // is thrown if the passed subject doesn't match the consumer's filter subject exactly.
        return js.subscribe(null, opts);
      } catch (IOException | JetStreamApiException e) {
        throw new RqueueNatsException(
            "Failed to bind pull subscription stream=" + stream + " consumer=" + consumerName, e);
      }
    });

    List<Message> msgs = sub.fetch(batch, fetchWait);
    List<RqueueMessage> out = new ArrayList<>(msgs.size());
    for (Message nm : msgs) {
      try {
        RqueueMessage rm = serdes.deserialize(nm.getData(), RqueueMessage.class);
        enrichFromDelivery(rm, nm);
        if (rm.getId() != null) {
          inFlight.put(inFlightKey(consumerName, rm.getId()), nm);
        }
        out.add(rm);
      } catch (RuntimeException | IOException e) {
        log.log(
            Level.WARNING,
            "Failed to deserialize JetStream payload on subject "
                + subject
                + "; nak'ing for redelivery",
            e);
        try {
          nm.nak();
        } catch (RuntimeException ignored) {
          // best-effort
        }
      }
    }
    return out;
  }

  @Override
  public boolean ack(QueueDetail q, RqueueMessage m) {
    if (m.getId() == null) {
      return false;
    }
    Message nm = inFlight.remove(inFlightKey(q.resolvedConsumerName(), m.getId()));
    if (nm == null) {
      return false;
    }
    nm.ack();
    return true;
  }

  /**
   * Extend the visibility timeout for a message that is still being processed. Sends a NATS
   * WIP (work-in-progress) signal to the server, which resets the consumer's {@code ackWait}
   * timer back to its configured value. Call this periodically from a long-running handler to
   * prevent JetStream from redelivering the message to another consumer while work is in flight.
   *
   * <p>The {@code deltaMs} hint from the caller is ignored — NATS always resets to the consumer's
   * fixed {@code ackWait}; there is no per-message extension API in JetStream.
   *
   * @return {@code true} if the WIP signal was sent; {@code false} if the message is no longer
   *         tracked (already acked, nacked, or the process restarted).
   */
  @Override
  public boolean extendVisibilityTimeout(QueueDetail q, RqueueMessage m, long deltaMs) {
    if (m.getId() == null) {
      return false;
    }
    Message nm = inFlight.get(inFlightKey(q.resolvedConsumerName(), m.getId()));
    if (nm == null) {
      return false;
    }
    try {
      nm.inProgress();
      return true;
    } catch (RuntimeException e) {
      log.log(
          Level.WARNING,
          "inProgress failed for message id=" + m.getId() + " queue=" + q.getName(),
          e);
      return false;
    }
  }

  @Override
  public boolean nack(QueueDetail q, RqueueMessage m, long retryDelayMs) {
    if (m.getId() == null) {
      return false;
    }
    Message nm = inFlight.remove(inFlightKey(q.resolvedConsumerName(), m.getId()));
    if (nm == null) {
      return false;
    }
    nm.nakWithDelay(Duration.ofMillis(Math.max(0L, retryDelayMs)));
    return true;
  }

  @Override
  public void moveToDlq(
      QueueDetail source,
      String targetQueue,
      RqueueMessage old,
      RqueueMessage updated,
      long delayMs) {
    // Ack the original NATS message so it is removed from the source stream.
    if (old.getId() != null) {
      Message nm = inFlight.remove(inFlightKey(source.resolvedConsumerName(), old.getId()));
      if (nm != null) {
        nm.ack();
      }
    }
    // targetQueue is the configured deadLetterQueue name (e.g. "job-morgue"). Map it to a NATS
    // stream and subject using the same prefix convention as any other queue.
    // NATS JetStream has no server-side delayed publish, so delayMs is ignored.
    String dlqStream = config.getStreamPrefix() + targetQueue;
    String dlqSubject = config.getSubjectPrefix() + targetQueue;
    Headers headers = new Headers();
    if (updated.getId() != null) {
      headers.add("Nats-Msg-Id", updated.getId() + "-dlq");
    }
    try {
      provisioner.ensureStream(
          dlqStream, List.of(dlqSubject), QueueType.QUEUE, "rqueue DLQ for queue: " + targetQueue);
      byte[] payload = serdes.serialize(updated);
      js.publish(dlqSubject, headers, payload);
    } catch (IOException | JetStreamApiException e) {
      throw new RqueueNatsException(
          "Failed to move message id=" + old.getId() + " to DLQ stream=" + dlqStream, e);
    } catch (RuntimeException e) {
      throw new RqueueNatsException(
          "Failed to serialize/publish message id=" + old.getId() + " to DLQ stream=" + dlqStream,
          e);
    }
  }

  @Override
  public long moveExpired(QueueDetail q, long now, int batch) {
    // No-op: JetStream's ack-wait + maxDeliver + DLQ advisory bridge handle redelivery and
    // dead-lettering. v1 capabilities advertise no scheduled introspection.
    return 0L;
  }

  @Override
  public List<RqueueMessage> peek(QueueDetail q, long offset, long count) {
    return peek(q, null, offset, count);
  }

  @Override
  public List<RqueueMessage> peek(QueueDetail q, String consumerName, long offset, long count) {
    String stream = streamFor(q);
    if (count <= 0) {
      return Collections.emptyList();
    }
    try {
      // Read messages directly from the stream by sequence number via the JetStream
      // Management API. This avoids creating any consumer, which sidesteps two NATS 2.12+
      // restrictions on WorkQueue-retention streams:
      //   1. Pull consumers require AckPolicy.Explicit (error 10084).
      //   2. Multiple consumers on a WorkQueue stream must be mutually exclusive via
      //      filter subjects (error 10100) — incompatible with the always-on durable
      //      consumer that the listener container uses.
      // Reading by sequence is purely non-destructive and works regardless of retention
      // policy or what other consumers exist on the stream.
      io.nats.client.api.StreamInfo info = jsm.getStreamInfo(stream);
      long firstSeq = info.getStreamState().getFirstSequence();
      long lastSeq = info.getStreamState().getLastSequence();
      if (lastSeq < firstSeq) {
        return Collections.emptyList();
      }
      // Consumer-aware base sequence for Limits-retention streams: when a consumerName is
      // provided, start from that consumer's lowest unacked sequence (ackFloor + 1) so the
      // dashboard shows everything this subscriber still has work to do on — both messages
      // already delivered but not yet acked (in-flight) and messages still to be delivered
      // (pending). Using delivered.streamSeq + 1 would hide the in-flight window, which
      // surprises operators who see "in-flight = 15" but get an empty explorer.
      // WorkQueue streams have a single shared consumer (msgs are removed on ack) so the
      // stream's firstSeq is already the right base — skip the lookup.
      long base = firstSeq;
      if (consumerName != null
          && !consumerName.isEmpty()
          && info.getConfiguration() != null
          && info.getConfiguration().getRetentionPolicy()
              == io.nats.client.api.RetentionPolicy.Limits) {
        try {
          io.nats.client.api.ConsumerInfo ci = jsm.getConsumerInfo(stream, consumerName);
          if (ci != null && ci.getAckFloor() != null) {
            base = Math.max(firstSeq, ci.getAckFloor().getStreamSequence() + 1);
          }
        } catch (JetStreamApiException ignore) {
          // consumer may have disappeared mid-walk; fall back to stream firstSeq
        }
      }
      long startSeq = base + Math.max(0L, offset);
      long endSeq = Math.min(lastSeq, startSeq + count - 1);
      List<RqueueMessage> out = new ArrayList<>();
      for (long seq = startSeq; seq <= endSeq && out.size() < count; seq++) {
        try {
          io.nats.client.api.MessageInfo mi = jsm.getMessage(stream, seq);
          if (mi == null || mi.getData() == null) {
            continue;
          }
          out.add(serdes.deserialize(mi.getData(), RqueueMessage.class));
        } catch (JetStreamApiException notFound) {
          // Sequence may have been purged or skipped (e.g. WorkQueue acks); keep walking.
          log.log(
              Level.FINE, "peek: skipping missing seq=" + seq + " on stream=" + stream, notFound);
        } catch (Exception deserErr) {
          log.log(Level.WARNING, "peek: skipping undeserializable seq=" + seq, deserErr);
        }
      }
      return out;
    } catch (IOException | JetStreamApiException e) {
      throw new RqueueNatsException(
          "Failed to peek queue=" + q.getName() + " offset=" + offset + " count=" + count, e);
    }
  }

  @Override
  public long size(QueueDetail q) {
    String stream = streamFor(q);
    try {
      io.nats.client.api.StreamInfo info = jsm.getStreamInfo(stream);
      io.nats.client.api.RetentionPolicy retention =
          info.getConfiguration() != null ? info.getConfiguration().getRetentionPolicy() : null;
      // WorkQueue retention removes messages on ack, so streamState.msgCount is the exact
      // count of outstanding work — the natural "pending size" for queue mode. For Limits
      // retention msgCount is the total retained messages regardless of consumer progress,
      // so we compute the worst-case outstanding work from stream position math:
      //   outstanding ≈ lastSeq - min(consumer.ackFloor.streamSeq)
      // which is the messages the slowest durable consumer has not yet acked. This matches
      // the per-consumer pending semantic in subscribers() (numPending + numAckPending) so
      // the queue-level "size" and the per-row pending counts agree on what "outstanding"
      // means.
      if (retention == io.nats.client.api.RetentionPolicy.Limits) {
        return approximateLimitsPending(stream, info);
      }
      return info.getStreamState().getMsgCount();
    } catch (IOException | JetStreamApiException e) {
      throw new RqueueNatsException("Failed to read stream size for queue=" + q.getName(), e);
    }
  }

  /**
   * Position-based outstanding-work estimate for a Limits-retention stream:
   * {@code lastSeq - min(consumer.ackFloor.streamSeq)} across all durable consumers — i.e. the
   * size of the unacked window for the slowest consumer (counts both yet-to-deliver and
   * delivered-but-unacked messages). Returns {@code msgCount} as a fallback when no consumers
   * exist or the enumeration fails, so the dashboard never misses a non-zero queue.
   */
  private long approximateLimitsPending(String stream, io.nats.client.api.StreamInfo info) {
    long lastSeq = info.getStreamState().getLastSequence();
    if (lastSeq <= 0) {
      return 0L;
    }
    try {
      List<String> consumers = jsm.getConsumerNames(stream);
      if (consumers == null || consumers.isEmpty()) {
        // No consumers attached: the entire retained range is outstanding from the perspective
        // of any future consumer. Stream's msgCount is the right approximation.
        return info.getStreamState().getMsgCount();
      }
      long minAckFloor = Long.MAX_VALUE;
      for (String consumer : consumers) {
        try {
          io.nats.client.api.ConsumerInfo ci = jsm.getConsumerInfo(stream, consumer);
          if (ci == null || ci.getAckFloor() == null) {
            continue;
          }
          long ackFloor = ci.getAckFloor().getStreamSequence();
          if (ackFloor < minAckFloor) {
            minAckFloor = ackFloor;
          }
        } catch (IOException | JetStreamApiException ignore) {
          // best-effort; skip consumers that disappear mid-walk
        }
      }
      if (minAckFloor == Long.MAX_VALUE) {
        return info.getStreamState().getMsgCount();
      }
      return Math.max(0L, lastSeq - minAckFloor);
    } catch (IOException | JetStreamApiException ignore) {
      return info.getStreamState().getMsgCount();
    }
  }

  /**
   * Reports whether {@link #size(QueueDetail)} is an approximation. True for Limits-retention
   * streams (per-consumer position math) and false for WorkQueue streams (msgCount is exact).
   */
  public boolean isSizeApproximate(QueueDetail q) {
    String stream = streamFor(q);
    try {
      io.nats.client.api.StreamInfo info = jsm.getStreamInfo(stream);
      io.nats.client.api.RetentionPolicy retention =
          info.getConfiguration() != null ? info.getConfiguration().getRetentionPolicy() : null;
      return retention == io.nats.client.api.RetentionPolicy.Limits;
    } catch (IOException | JetStreamApiException e) {
      return false;
    }
  }

  /**
   * Per-consumer subscriber view used by the queue-detail dashboard. Walks all durable
   * consumers on the queue's stream and reports each one's pending + in-flight counts as
   * separate columns — same split as the Redis backend's processing ZSET vs ready LIST.
   *
   * <p><b>Pending semantics.</b> {@code pending} is yet-to-deliver work for this consumer.
   * For WorkQueue retention this is the stream's shared {@code msgCount} (every row shows the
   * same number, marked {@code pendingShared = true}); for Limits retention it is the
   * consumer's exact {@code numPending}. {@code inFlight} is always the consumer's exclusive
   * {@code numAckPending}: messages delivered but not yet acked. The two are disjoint —
   * {@code pending} excludes anything currently in flight — so an operator reading the row
   * sees the work split between "still to dispatch" and "currently being processed". Total
   * outstanding work for the consumer is the sum of the two, which is what the explorer
   * surfaces when the operator clicks the consumer link.
   *
   * <p>If consumer enumeration fails or the stream is unprovisioned, falls back to the
   * default single-row implementation so the dashboard still renders something useful.
   */
  @Override
  public java.util.List<com.github.sonus21.rqueue.core.spi.SubscriberView> subscribers(
      QueueDetail q) {
    String stream = streamFor(q);
    try {
      io.nats.client.api.StreamInfo info = jsm.getStreamInfo(stream);
      io.nats.client.api.RetentionPolicy retention =
          info.getConfiguration() != null ? info.getConfiguration().getRetentionPolicy() : null;
      boolean pendingIsShared = retention != io.nats.client.api.RetentionPolicy.Limits;
      long sharedPending = info.getStreamState().getMsgCount();
      List<String> consumers = jsm.getConsumerNames(stream);
      if (consumers == null || consumers.isEmpty()) {
        return com.github.sonus21.rqueue.core.spi.MessageBroker.super.subscribers(q);
      }
      java.util.List<com.github.sonus21.rqueue.core.spi.SubscriberView> out =
          new java.util.ArrayList<>(consumers.size());
      for (String consumer : consumers) {
        try {
          io.nats.client.api.ConsumerInfo ci = jsm.getConsumerInfo(stream, consumer);
          if (ci == null) {
            continue;
          }
          long pending = pendingIsShared ? sharedPending : ci.getNumPending();
          long inFlight = ci.getNumAckPending();
          out.add(new com.github.sonus21.rqueue.core.spi.SubscriberView(
              consumer, pending, inFlight, pendingIsShared));
        } catch (IOException | JetStreamApiException ignore) {
          // best-effort; skip consumers that disappear mid-walk
        }
      }
      if (out.isEmpty()) {
        return com.github.sonus21.rqueue.core.spi.MessageBroker.super.subscribers(q);
      }
      return out;
    } catch (IOException | JetStreamApiException e) {
      log.log(Level.WARNING, "subscribers() failed for stream=" + stream, e);
      return com.github.sonus21.rqueue.core.spi.MessageBroker.super.subscribers(q);
    }
  }

  /**
   * For Limits-retention streams, returns an exact per-consumer pending count
   * ({@code lastSeq - delivered.streamSeq}). For WorkQueue streams returns {@code null} so the
   * dashboard falls back to the single {@link #size(QueueDetail)} row — WorkQueue messages are
   * shared across consumers, so a per-consumer split is meaningless.
   *
   * <p>The map iteration order matches {@link io.nats.client.JetStreamManagement#getConsumerNames}
   * (insertion order), giving the dashboard a stable rendering.
   */
  @Override
  @Deprecated
  public java.util.Map<String, Long> consumerPendingSizes(QueueDetail q) {
    String stream = streamFor(q);
    try {
      io.nats.client.api.StreamInfo info = jsm.getStreamInfo(stream);
      io.nats.client.api.RetentionPolicy retention =
          info.getConfiguration() != null ? info.getConfiguration().getRetentionPolicy() : null;
      if (retention != io.nats.client.api.RetentionPolicy.Limits) {
        // WorkQueue (and any future single-pool retention) doesn't have per-consumer pending.
        return null;
      }
      long lastSeq = info.getStreamState().getLastSequence();
      List<String> consumers = jsm.getConsumerNames(stream);
      if (consumers == null || consumers.isEmpty()) {
        return java.util.Collections.emptyMap();
      }
      java.util.Map<String, Long> out = new java.util.LinkedHashMap<>();
      for (String consumer : consumers) {
        try {
          io.nats.client.api.ConsumerInfo ci = jsm.getConsumerInfo(stream, consumer);
          if (ci == null) {
            continue;
          }
          // Prefer numPending when available (server-computed); fall back to position math.
          long pending = ci.getNumPending();
          if (pending == 0 && ci.getDelivered() != null) {
            long delivered = ci.getDelivered().getStreamSequence();
            pending = Math.max(0L, lastSeq - delivered);
          }
          out.put(consumer, pending);
        } catch (IOException | JetStreamApiException ignore) {
          // best-effort; skip consumers that disappear mid-walk
        }
      }
      return out;
    } catch (IOException | JetStreamApiException e) {
      log.log(Level.WARNING, "consumerPendingSizes failed for stream=" + stream, e);
      return null;
    }
  }

  @Override
  public AutoCloseable subscribe(String channel, Consumer<String> handler) {
    Dispatcher d =
        connection.createDispatcher(msg -> handler.accept(new String(msg.getData(), UTF_8)));
    d.subscribe(channel);
    return () -> {
      try {
        connection.closeDispatcher(d);
      } catch (RuntimeException e) {
        // best-effort close
      }
    };
  }

  @Override
  public void publish(String channel, String payload) {
    connection.publish(channel, payload.getBytes(UTF_8));
  }

  @Override
  public void onQueueRegistered(QueueDetail q) {
    String stream = streamFor(q);
    String subject = subjectFor(q);
    provisioner.ensureStream(stream, List.of(subject), q.getType(), streamDescription(q));
  }

  /**
   * NATS subjects use {@code .} as a hierarchy separator and stream / consumer names disallow it
   * outright. A queue name like {@code "orders.us"} would (a) silently turn the publish subject
   * into a two-level token tree and (b) make {@code rqueue-js-orders.us} an invalid stream name —
   * the JetStream API would reject it with an opaque error at first publish. Reject the name at
   * registration so the failure is loud and local.
   *
   * <p>{@code *} and {@code &gt;} are also illegal in subject tokens (they're NATS wildcards), and
   * whitespace is rejected by the server; check those too while we're here.
   */
  @Override
  public void validateQueueName(String queueName) {
    if (queueName == null || queueName.isEmpty()) {
      return;
    }
    for (int i = 0; i < queueName.length(); i++) {
      char c = queueName.charAt(i);
      if (c == '.' || c == '*' || c == '>' || Character.isWhitespace(c)) {
        throw new IllegalArgumentException("Queue name '"
            + queueName
            + "' contains illegal character '"
            + c
            + "' for the NATS backend. Subject hierarchy ('.'), wildcards ('*', '>') and"
            + " whitespace are not allowed in queue names — use '-' or '_' instead.");
      }
    }
  }

  @Override
  public Capabilities capabilities() {
    return caps;
  }

  /**
   * Headers for a scheduled (or periodic) JetStream publish.
   *
   * <p><b>Dedup key strategy — {@code Nats-Msg-Id}:</b>
   * <ul>
   *   <li>Scheduled messages ({@code processAt > 0}) — key is {@code id@processAt}.
   *       Each period of a recurring message has a unique {@code processAt} (advances by
   *       {@code period} each time), so consecutive periods never share a key and are never
   *       suppressed. Retries of the same period reuse the identical {@code processAt}, so
   *       the duplicate {@code scheduleNext} publish is correctly deduplicated by JetStream —
   *       preventing double-execution of a period when the handler fails and is redelivered.
   *   <li>Non-scheduled messages ({@code processAt == 0}) — plain {@code m.getId()}; each
   *       enqueue generates a fresh UUID so there is no collision risk.
   * </ul>
   *
   * <p><b>Enrichment headers</b> — written at publish time so they can be read back on
   * {@link #popInternal} without deserializing the payload:
   * <ul>
   *   <li>{@code Rqueue-Process-At} — epoch-ms at which the message should be processed;
   *       used to set {@link RqueueMessage#setProcessAt} if the deserialized payload lacks it.
   *   <li>{@code Rqueue-Period} — period in ms for periodic messages; used to set
   *       {@link RqueueMessage#setPeriod} if the deserialized payload lacks it.
   * </ul>
   */
  /**
   * Build NATS message-scheduling headers for a delayed publish (ADR-51, NATS >= 2.12).
   *
   * <p>The message is published to a dedicated scheduler subject
   * ({@code <workSubject>.sched.<msgId>}). When the schedule triggers, NATS fires a
   * JetStream publish to {@code workSubject} so consumers see it only after the delay.
   *
   * <p>Required headers (confirmed against nats-server 2.12.8):
   * <ul>
   *   <li>{@code Nats-Schedule: @at <RFC3339-UTC>} — trigger time, {@code @at } prefix required
   *   <li>{@code Nats-Schedule-Target: <workSubject>} — where to publish when schedule fires
   *   <li>{@code Nats-Rollup: sub} — replaces any existing schedule for this subject (idempotent)
   * </ul>
   */
  private Headers buildSchedulingHeaders(RqueueMessage m, long delayMs, String workSubject) {
    Headers headers = new Headers();
    if (m.getId() != null) {
      // Dedup key: id-at-processAt for scheduled messages (processAt > 0).
      //   - Each period of a recurring message has a unique processAt → unique key → no
      //     cross-period suppression.
      //   - If scheduleNext is called again for the same period on redelivery after a handler
      //     failure, processAt is identical → same key → JetStream deduplicates the second
      //     publish and the period executes exactly once.
      // For non-scheduled messages (processAt == 0) the plain id is used; each enqueue
      // generates a fresh UUID so there is no collision risk.
      String dedupKey = m.getProcessAt() > 0 ? m.getId() + "-at-" + m.getProcessAt() : m.getId();
      headers.add("Nats-Msg-Id", dedupKey);
    }
    long deliverAtMs =
        m.getProcessAt() > 0 ? m.getProcessAt() : System.currentTimeMillis() + delayMs;
    // "@at " prefix is required — bare RFC3339 is not recognised by the server scheduler.
    String deliverAt = "@at " + RFC3339_UTC.format(Instant.ofEpochMilli(deliverAtMs));
    headers.add(HDR_SCHEDULE, deliverAt);
    headers.add(HDR_SCHEDULE_TARGET, workSubject);
    // Rollup-sub: replaces any existing schedule entry for this scheduler subject so that
    // re-enqueue of the same message ID at the same processAt is idempotent.
    headers.add("Nats-Rollup", "sub");
    // Enrichment headers — readable on pop without deserializing the payload
    headers.add(HDR_PROCESS_AT, String.valueOf(deliverAtMs));
    if (m.isPeriodic()) {
      headers.add(HDR_PERIOD, String.valueOf(m.getPeriod()));
    }
    return headers;
  }

  /**
   * Enrich a deserialized {@link RqueueMessage} with delivery metadata from the NATS message.
   *
   * <p><b>Failure count</b> — derived from {@code metaData().deliveredCount() - 1} (JetStream
   * tracks redeliveries in the reply-to subject). This is the authoritative source; we never
   * re-publish on retry, so a static header set at publish time would always show 0.
   *
   * <p><b>Scheduling fields</b> — {@code processAt} and {@code period} are read from the
   * enrichment headers ({@code Rqueue-Process-At}, {@code Rqueue-Period}) when the deserialized
   * message has them as zero. This handles payloads published by older broker versions that
   * predate per-field population, or any case where the JSON payload was trimmed.
   */
  private static void enrichFromDelivery(RqueueMessage rm, Message nm) {
    // Failure count from JetStream redelivery metadata (the authoritative retry counter).
    try {
      long deliveredCount = nm.metaData().deliveredCount();
      rm.setFailureCount((int) Math.max(0, deliveredCount - 1));
    } catch (Exception ignored) {
      // defensive: metadata absent on non-JetStream or synthetic messages
    }
    // Scheduling fields from publish-time enrichment headers.
    if (rm.getProcessAt() <= 0) {
      String processAtHdr =
          nm.getHeaders() == null ? null : nm.getHeaders().getFirst(HDR_PROCESS_AT);
      if (processAtHdr != null) {
        try {
          rm.setProcessAt(Long.parseLong(processAtHdr));
        } catch (NumberFormatException ignored) {
          // malformed header; leave processAt as-is
        }
      }
    }
    if (!rm.isPeriodic() && nm.getHeaders() != null) {
      String periodHdr = nm.getHeaders().getFirst(HDR_PERIOD);
      if (periodHdr != null) {
        try {
          long period = Long.parseLong(periodHdr);
          if (period > 0) {
            rm.setPeriod(period);
          }
        } catch (NumberFormatException ignored) {
          // malformed header; leave period as-is
        }
      }
    }
  }

  @Override
  public String storageKicker() {
    return "NATS";
  }

  @Override
  public String storageDescription() {
    return "Underlying NATS JetStream streams for the queues visible on this page.";
  }

  @Override
  public String storageDisplayName(QueueDetail q) {
    return streamFor(q);
  }

  @Override
  public String dlqStorageDisplayName(QueueDetail q) {
    return dlqStreamFor(q);
  }

  /**
   * Map the dashboard's Redis-shaped data-type tokens onto NATS terminology. Each per-queue
   * stream uses the JetStream {@code WorkQueue} retention policy by default, so the pending
   * row is labelled "Queue (Stream)". Completed messages are tracked in a KV bucket and DLQs
   * are independent streams.
   */
  @Override
  public String dataTypeLabel(
      com.github.sonus21.rqueue.models.enums.NavTab tab,
      com.github.sonus21.rqueue.models.enums.DataType type) {
    if (tab == null) {
      return type == null ? null : "Stream";
    }
    switch (tab) {
      case PENDING:
        return "Queue (Stream)";
      case DEAD:
        return "Dead Letter (Stream)";
      case COMPLETED:
        return "Completed (KV)";
      default:
        // Running / Scheduled / Cron tabs are hidden on NATS via Capabilities; fall through.
        return type == null ? null : "Stream";
    }
  }

  @Override
  public void close() {
    for (JetStreamSubscription s : subscriptionCache.values()) {
      try {
        s.unsubscribe();
      } catch (RuntimeException ignored) {
        // ignore
      }
    }
    subscriptionCache.clear();
    inFlight.clear();
  }

  /**
   * Provision a DLQ stream for the given queue. Caller wires up an advisory listener (subscribed to
   * {@code $JS.EVENT.ADVISORY.CONSUMER.MAX_DELIVERIES.>}) that republishes the exhausted message
   * onto {@link #dlqSubjectFor(QueueDetail)}. v1 leaves the bridge wiring opt-in via
   * {@link #installDeadLetterBridge(QueueDetail, String)}.
   */
  public void provisionDlq(QueueDetail q) {
    // Explicit call — always provision, bypassing the autoCreateDlqStream flag.
    // That flag gates automatic provisioning at bootstrap; here the caller is explicitly opting in.
    provisioner.ensureStream(
        dlqStreamFor(q), List.of(dlqSubjectFor(q)), QueueType.QUEUE, dlqStreamDescription(q));
  }

  /**
   * Install a background dispatcher that watches max-deliveries advisories on the queue's stream
   * and republishes the offending payload onto the DLQ subject. Returns an {@link AutoCloseable}
   * that tears the dispatcher down. Tests rely on this; production code in Phase 3 will call it
   * during container start.
   */
  public AutoCloseable installDeadLetterBridge(QueueDetail q, String consumerName) {
    provisionDlq(q);
    String advisorySubject =
        "$JS.EVENT.ADVISORY.CONSUMER.MAX_DELIVERIES." + streamFor(q) + "." + consumerName;
    String dlqSubject = dlqSubjectFor(q);
    String stream = streamFor(q);
    Dispatcher d = connection.createDispatcher(advisoryMsg -> {
      try {
        @SuppressWarnings("unchecked")
        Map<String, Object> adv = serdes.deserialize(advisoryMsg.getData(), Map.class);
        Object seqVal = adv.get("stream_seq");
        long streamSeq = seqVal instanceof Number ? ((Number) seqVal).longValue() : -1L;
        if (streamSeq <= 0) {
          return;
        }
        io.nats.client.api.MessageInfo mi = jsm.getMessage(stream, streamSeq);
        Headers h = new Headers();
        if (mi.getHeaders() != null) {
          mi.getHeaders().forEach((k, v) -> h.add(k, v));
        }
        js.publish(dlqSubject, h, mi.getData());
      } catch (Exception e) {
        log.log(
            Level.WARNING, "Failed to bridge max-delivery advisory to DLQ for stream=" + stream, e);
      }
    });
    d.subscribe(advisorySubject);
    return () -> {
      try {
        connection.closeDispatcher(d);
      } catch (RuntimeException ignored) {
        // best-effort
      }
    };
  }

  // ---- builder -----------------------------------------------------------

  public static class Builder {

    private Connection connection;
    private JetStream jetStream;
    private JetStreamManagement management;
    private RqueueNatsConfig config;
    private RqueueSerDes serdes;
    private NatsProvisioner provisioner;

    public Builder connection(Connection connection) {
      this.connection = connection;
      return this;
    }

    public Builder jetStream(JetStream jetStream) {
      this.jetStream = jetStream;
      return this;
    }

    public Builder management(JetStreamManagement management) {
      this.management = management;
      return this;
    }

    public Builder config(RqueueNatsConfig config) {
      this.config = config;
      return this;
    }

    public Builder serDes(RqueueSerDes serdes) {
      this.serdes = serdes;
      return this;
    }

    public Builder provisioner(NatsProvisioner provisioner) {
      this.provisioner = provisioner;
      return this;
    }

    public JetStreamMessageBroker build() {
      if (connection == null) {
        throw new IllegalStateException("connection is required");
      }
      try {
        if (jetStream == null) {
          jetStream = connection.jetStream();
        }
        if (management == null) {
          management = connection.jetStreamManagement();
        }
        if (provisioner == null) {
          provisioner = new NatsProvisioner(
              connection, management, config != null ? config : RqueueNatsConfig.defaults());
        }
      } catch (IOException e) {
        throw new RqueueNatsException("Failed to derive JetStream context from connection", e);
      }
      if (config == null) {
        config = RqueueNatsConfig.defaults();
      }
      if (serdes == null) {
        serdes = new RqJacksonSerDes(SerializationUtils.getObjectMapper());
      }
      return new JetStreamMessageBroker(
          connection, jetStream, management, config, serdes, provisioner);
    }

    /**
     * Create a broker that wraps a pre-built {@link Map} of NATS handles. Used by the factory.
     */
    public JetStreamMessageBroker buildFromConfig(Map<String, String> ignored) {
      return build();
    }
  }
}
