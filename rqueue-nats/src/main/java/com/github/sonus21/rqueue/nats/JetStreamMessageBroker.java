/*
 * Copyright (c) 2024-2026 Sonu Kumar
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * You may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 */
package com.github.sonus21.rqueue.nats;

import static java.nio.charset.StandardCharsets.UTF_8;

import com.github.sonus21.rqueue.core.RqueueMessage;
import com.github.sonus21.rqueue.core.spi.Capabilities;
import com.github.sonus21.rqueue.core.spi.MessageBroker;
import com.github.sonus21.rqueue.listener.QueueDetail;
import com.github.sonus21.rqueue.nats.internal.NatsProvisioner;
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
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;
import java.util.logging.Level;
import java.util.logging.Logger;
import reactor.core.publisher.Mono;
import tools.jackson.databind.ObjectMapper;

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

  private final Connection connection;
  private final JetStream js;
  private final JetStreamManagement jsm;
  private final RqueueNatsConfig config;
  private final ObjectMapper mapper;
  private final NatsProvisioner provisioner;

  /** keyed by RqueueMessage.id, value is the underlying NATS Message for ack/nak. */
  private final ConcurrentHashMap<String, Message> inFlight = new ConcurrentHashMap<>();

  /** Cached pull subscriptions keyed by stream + consumerName so we don't re-bind on every pop. */
  private final ConcurrentHashMap<String, JetStreamSubscription> subscriptionCache =
      new ConcurrentHashMap<>();

  JetStreamMessageBroker(
      Connection connection,
      JetStream js,
      JetStreamManagement jsm,
      RqueueNatsConfig config,
      ObjectMapper mapper) {
    this.connection = connection;
    this.js = js;
    this.jsm = jsm;
    this.config = config;
    this.mapper = mapper;
    this.provisioner = new NatsProvisioner(jsm, config);
  }

  public static Builder builder() {
    return new Builder();
  }

  // ---- subject / stream naming -------------------------------------------

  // TODO: once Phase 1 lands, read additive QueueDetail.getNatsSubject() / getNatsStream() if set.
  private String subjectFor(QueueDetail q) {
    return config.getSubjectPrefix() + q.getName();
  }

  private String streamFor(QueueDetail q) {
    return config.getStreamPrefix() + q.getName();
  }

  /**
   * Resolve the priority-specific subject. Returns the unsuffixed subject when {@code priority}
   * is null or empty; otherwise appends {@code "." + priority}. Mirrors the naming used by
   * {@link QueueDetail#resolvedNatsSubjectForPriority(String)}.
   */
  private String subjectFor(QueueDetail q, String priority) {
    if (priority == null || priority.isEmpty()) {
      return subjectFor(q);
    }
    return subjectFor(q) + "." + priority;
  }

  /**
   * Resolve the priority-specific stream. Returns the unsuffixed stream when {@code priority}
   * is null or empty; otherwise appends {@code "-" + priority}.
   */
  private String streamFor(QueueDetail q, String priority) {
    if (priority == null || priority.isEmpty()) {
      return streamFor(q);
    }
    return streamFor(q) + "-" + priority;
  }

  private String dlqStreamFor(QueueDetail q) {
    return streamFor(q) + config.getDlqStreamSuffix();
  }

  private String dlqSubjectFor(QueueDetail q) {
    return subjectFor(q) + config.getDlqSubjectSuffix();
  }

  // ---- MessageBroker -----------------------------------------------------

  @Override
  public void enqueue(QueueDetail q, RqueueMessage m) {
    String subject = subjectFor(q);
    provisioner.ensureStream(streamFor(q), List.of(subject));
    Headers headers = new Headers();
    if (m.getId() != null) {
      headers.add("Nats-Msg-Id", m.getId());
    }
    try {
      byte[] payload = mapper.writeValueAsBytes(m);
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
    String stream = streamFor(q, priority);
    String subject = subjectFor(q, priority);
    provisioner.ensureStream(stream, List.of(subject));
    Headers headers = new Headers();
    if (m.getId() != null) {
      headers.add("Nats-Msg-Id", m.getId());
    }
    try {
      byte[] payload = mapper.writeValueAsBytes(m);
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
    Headers headers = new Headers();
    if (m.getId() != null) {
      headers.add("Nats-Msg-Id", m.getId());
    }
    byte[] payload;
    try {
      payload = mapper.writeValueAsBytes(m);
    } catch (RuntimeException e) {
      return Mono.error(new RqueueNatsException(
          "Failed to serialize message id="
              + m.getId()
              + " queue="
              + q.getName()
              + " subject="
              + subject,
          e));
    }
    return Mono.fromRunnable(() -> provisioner.ensureStream(streamFor(q), List.of(subject)))
        .then(Mono.fromFuture(() -> js.publishAsync(subject, headers, payload)))
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
    return popInternal(streamFor(q), subjectFor(q), consumerName, batch, wait);
  }

  @Override
  public List<RqueueMessage> pop(
      QueueDetail q, String priority, String consumerName, int batch, Duration wait) {
    return popInternal(streamFor(q, priority), subjectFor(q, priority), consumerName, batch, wait);
  }

  private List<RqueueMessage> popInternal(
      String stream, String subject, String consumerName, int batch, Duration wait) {
    provisioner.ensureStream(stream, List.of(subject));
    provisioner.ensureConsumer(
        stream,
        consumerName,
        config.getConsumerDefaults().getAckWait(),
        config.getConsumerDefaults().getMaxDeliver(),
        config.getConsumerDefaults().getMaxAckPending(),
        subject);
    Duration fetchWait = wait != null ? wait : config.getDefaultFetchWait();
    String key = stream + "/" + consumerName;
    JetStreamSubscription sub = subscriptionCache.computeIfAbsent(key, k -> {
      try {
        PullSubscribeOptions opts = PullSubscribeOptions.bind(stream, consumerName);
        return js.subscribe(subject, opts);
      } catch (IOException | JetStreamApiException e) {
        throw new RqueueNatsException(
            "Failed to bind pull subscription stream=" + stream + " consumer=" + consumerName, e);
      }
    });

    List<Message> msgs = sub.fetch(batch, fetchWait);
    List<RqueueMessage> out = new ArrayList<>(msgs.size());
    for (Message nm : msgs) {
      try {
        RqueueMessage rm = mapper.readValue(nm.getData(), RqueueMessage.class);
        if (rm.getId() != null) {
          inFlight.put(rm.getId(), nm);
        }
        out.add(rm);
      } catch (RuntimeException e) {
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
  public long moveExpired(QueueDetail q, long now, int batch) {
    // No-op: JetStream's ack-wait + maxDeliver + DLQ advisory bridge handle redelivery and
    // dead-lettering. v1 capabilities advertise no scheduled introspection.
    return 0L;
  }

  @Override
  public List<RqueueMessage> peek(QueueDetail q, long offset, long count) {
    String stream = streamFor(q);
    String subject = subjectFor(q);
    provisioner.ensureStream(stream, List.of(subject));
    JetStreamSubscription sub = null;
    try {
      ConsumerConfiguration.Builder cb = ConsumerConfiguration.builder()
          .ackPolicy(AckPolicy.None)
          .filterSubject(subject)
          .name("rqueue-peek-" + UUID.randomUUID());
      if (offset > 0) {
        cb.deliverPolicy(DeliverPolicy.ByStartSequence).startSequence(Math.max(1L, offset));
      } else {
        cb.deliverPolicy(DeliverPolicy.All);
      }
      PullSubscribeOptions opts = PullSubscribeOptions.builder().stream(stream)
          .configuration(cb.build())
          .build();
      sub = js.subscribe(subject, opts);
      int n = (int) Math.min(Integer.MAX_VALUE, Math.max(0L, count));
      List<Message> msgs = sub.fetch(n, Duration.ofSeconds(2));
      List<RqueueMessage> out = new ArrayList<>(msgs.size());
      for (Message nm : msgs) {
        try {
          out.add(mapper.readValue(nm.getData(), RqueueMessage.class));
        } catch (Exception e) {
          log.log(Level.WARNING, "peek: skipping undeserializable message", e);
        }
      }
      return out;
    } catch (IOException | JetStreamApiException e) {
      throw new RqueueNatsException(
          "Failed to peek queue=" + q.getName() + " offset=" + offset + " count=" + count, e);
    } finally {
      if (sub != null) {
        try {
          sub.unsubscribe();
        } catch (RuntimeException ignored) {
          // ephemeral consumer is auto-reaped server-side; ignore
        }
      }
    }
  }

  @Override
  public long size(QueueDetail q) {
    String stream = streamFor(q);
    try {
      return jsm.getStreamInfo(stream).getStreamState().getMsgCount();
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
  public Capabilities capabilities() {
    return CAPS;
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
   * onto {@link #dlqSubjectFor(QueueDetail)}. v1 leaves the bridge wiring opt-in via {@link
   * #installDeadLetterBridge(QueueDetail, String)}.
   */
  public void provisionDlq(QueueDetail q) {
    if (!config.isAutoCreateDlqStream()) {
      return;
    }
    provisioner.ensureDlqStream(dlqStreamFor(q), List.of(dlqSubjectFor(q)));
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
        tools.jackson.databind.JsonNode adv = mapper.readTree(advisoryMsg.getData());
        long streamSeq = adv.path("stream_seq").asLong(-1);
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
    private ObjectMapper mapper;

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

    public Builder objectMapper(ObjectMapper mapper) {
      this.mapper = mapper;
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
      } catch (IOException e) {
        throw new RqueueNatsException("Failed to derive JetStream context from connection", e);
      }
      if (config == null) {
        config = RqueueNatsConfig.defaults();
      }
      if (mapper == null) {
        mapper = new ObjectMapper();
      }
      return new JetStreamMessageBroker(connection, jetStream, management, config, mapper);
    }

    /** Create a broker that wraps a pre-built {@link Map} of NATS handles. Used by the factory. */
    public JetStreamMessageBroker buildFromConfig(Map<String, String> ignored) {
      return build();
    }
  }
}
