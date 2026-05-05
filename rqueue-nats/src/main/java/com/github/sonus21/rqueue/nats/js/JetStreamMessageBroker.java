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
import io.nats.client.api.AckPolicy;
import io.nats.client.api.ConsumerConfiguration;
import io.nats.client.api.DeliverPolicy;
import io.nats.client.impl.Headers;
import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;
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
  private static final Capabilities CAPS = new Capabilities(false, false, false, false);

  /**
   * Lower bound for fetch wait when the caller passes a non-positive duration. JetStream rejects
   * zero on a pull fetch, so any zero/negative wait is rounded up to this minimum. Callers that
   * want long-poll semantics should pass the desired wait explicitly (e.g. the listener
   * container's {@code pollingInterval}); this constant only guards against accidental zero waits
   * from non-listener callers.
   */
  private static final Duration MIN_FETCH_WAIT = Duration.ofMillis(50);

  private final Connection connection;
  private final JetStream js;
  private final JetStreamManagement jsm;
  private final RqueueNatsConfig config;
  private final RqueueSerDes serdes;
  private final NatsProvisioner provisioner;

  /**
   * keyed by RqueueMessage.id, value is the underlying NATS Message for ack/nak.
   */
  private final ConcurrentHashMap<String, Message> inFlight = new ConcurrentHashMap<>();

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
    throw new UnsupportedOperationException(
        "delayed enqueue not supported by NATS backend in this version; "
            + "use the Redis backend for scheduled messages");
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
    return Mono.error(new UnsupportedOperationException(
        "delayed enqueue not supported by NATS backend in this version; "
            + "use the Redis backend for scheduled messages"));
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
            ackWait,
            maxDeliver,
            config.getConsumerDefaults().getMaxAckPending());
        PullSubscribeOptions opts = PullSubscribeOptions.bind(stream, actualConsumerName);
        // Consumer has no filter subject; pass null so the NATS client doesn't validate
        // the subject against a (nonexistent) filter — SUB-90011 otherwise.
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
        // derive failure count from JetStream redelivery metadata
        try {
          long deliveredCount = nm.metaData().deliveredCount();
          rm.setFailureCount((int) Math.max(0, deliveredCount - 1));
        } catch (Exception ignored) {
          // defensive: metadata unavailable on non-JetStream messages
        }
        if (rm.getId() != null) {
          inFlight.put(rm.getId(), nm);
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
    Message nm = inFlight.remove(m.getId());
    if (nm == null) {
      return false;
    }
    nm.ack();
    return true;
  }

  @Override
  public boolean nack(QueueDetail q, RqueueMessage m, long retryDelayMs) {
    if (m.getId() == null) {
      return false;
    }
    Message nm = inFlight.remove(m.getId());
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
      Message nm = inFlight.remove(old.getId());
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
      long startSeq = Math.max(firstSeq, firstSeq + Math.max(0L, offset));
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
              Level.FINE,
              "peek: skipping missing seq=" + seq + " on stream=" + stream,
              notFound);
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
      // WorkQueue retention removes messages on ack, so streamState.msgCount equals the count
      // of messages still pending consumption — the natural "pending size" for queue-mode use.
      // For Limits retention, msgCount is the total messages currently stored regardless of
      // consumer progress, so falling back to it would over-report. In that case, walk the
      // stream's durable consumers and surface the maximum unacked-pending count, which best
      // approximates "messages still to be processed by someone" for the dashboard.
      if (retention == io.nats.client.api.RetentionPolicy.Limits) {
        long maxPending = 0L;
        try {
          List<String> consumers = jsm.getConsumerNames(stream);
          for (String consumer : consumers) {
            try {
              io.nats.client.api.ConsumerInfo ci = jsm.getConsumerInfo(stream, consumer);
              if (ci != null && ci.getNumPending() > maxPending) {
                maxPending = ci.getNumPending();
              }
            } catch (IOException | JetStreamApiException ignore) {
              // best-effort; skip consumers that disappear mid-walk
            }
          }
          return maxPending;
        } catch (IOException | JetStreamApiException ignore) {
          // Fallback to stream count if the consumer enumeration fails — better than throwing
          // for a dashboard read.
          return info.getStreamState().getMsgCount();
        }
      }
      return info.getStreamState().getMsgCount();
    } catch (IOException | JetStreamApiException e) {
      throw new RqueueNatsException("Failed to read stream size for queue=" + q.getName(), e);
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
    return CAPS;
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
