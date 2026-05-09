/*
 * Copyright (c) 2026 Sonu Kumar
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * You may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 */
package com.github.sonus21.rqueue.nats.internal;

import com.github.sonus21.rqueue.enums.QueueType;
import com.github.sonus21.rqueue.nats.RqueueNatsConfig;
import com.github.sonus21.rqueue.nats.RqueueNatsException;
import io.nats.client.Connection;
import io.nats.client.JetStreamApiException;
import io.nats.client.JetStreamManagement;
import io.nats.client.KeyValue;
import io.nats.client.KeyValueManagement;
import io.nats.client.api.AckPolicy;
import io.nats.client.api.CompressionOption;
import io.nats.client.api.ConsumerConfiguration;
import io.nats.client.api.ConsumerInfo;
import io.nats.client.api.DeliverPolicy;
import io.nats.client.api.KeyValueConfiguration;
import io.nats.client.api.KeyValueStatus;
import io.nats.client.api.RetentionPolicy;
import io.nats.client.api.StreamConfiguration;
import io.nats.client.api.StreamInfo;
import java.io.IOException;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Idempotent stream/consumer/KV provisioning helpers. All methods are safe to call repeatedly;
 * if the target object already exists with a compatible config, this class leaves it alone. If it
 * exists but with diverging config (e.g. ackWait, maxDeliver), this class logs a WARN and does
 * NOT auto-mutate so user customizations are preserved.
 */
public class NatsProvisioner {

  private static final Logger log = Logger.getLogger(NatsProvisioner.class.getName());

  private final Connection connection;
  private final KeyValueManagement kvm;
  private final JetStreamManagement jsm;
  private final RqueueNatsConfig config;

  private final ConcurrentHashMap<String, KeyValue> kvCache = new ConcurrentHashMap<>();
  private final ConcurrentHashMap<String, Object> kvLocks = new ConcurrentHashMap<>();

  // stream name → schedulingEnabled (true if the stream was created/updated with
  // allowMessageSchedules)
  private final ConcurrentHashMap<String, Boolean> streamsDone = new ConcurrentHashMap<>();
  private final ConcurrentHashMap<String, Object> streamLocks = new ConcurrentHashMap<>();

  // "streamName/requestedConsumerName" → actual consumer name (may differ for stale-rebind)
  private final ConcurrentHashMap<String, String> consumerCache = new ConcurrentHashMap<>();
  private final ConcurrentHashMap<String, Object> consumerLocks = new ConcurrentHashMap<>();

  /**
   * Minimum NATS server version that supports server-side message scheduling via the
   * {@code Nats-Schedule} JetStream publish header (ADR-51).
   */
  public static final String SCHEDULING_MIN_VERSION = "2.12.0";

  private final boolean schedulingSupported;

  public NatsProvisioner(Connection connection, JetStreamManagement jsm, RqueueNatsConfig config)
      throws IOException {
    this.connection = connection;
    this.kvm = connection.keyValueManagement();
    this.jsm = jsm;
    this.config = config;
    io.nats.client.api.ServerInfo serverInfo = connection.getServerInfo();
    this.schedulingSupported =
        serverInfo != null && serverInfo.isSameOrNewerThanVersion(SCHEDULING_MIN_VERSION);
    log.log(
        Level.INFO,
        "NATS server version={0}; message scheduling (ADR-51) supported={1}",
        new Object[] {serverInfo != null ? serverInfo.getVersion() : "unknown", schedulingSupported
        });
  }

  /** Returns {@code true} when the connected NATS server supports message scheduling (>= 2.12). */
  public boolean isMessageSchedulingSupported() {
    return schedulingSupported;
  }

  // ---- KV provisioning --------------------------------------------------

  /**
   * Returns a {@link KeyValue} handle for {@code bucketName}, creating the bucket on first call.
   * All buckets are created with S2 compression. A bucket-level TTL is applied at creation time
   * only; existing buckets are reused as-is.
   *
   * @param bucketName NATS KV bucket name (e.g. {@code "rqueue-jobs"})
   * @param ttl        bucket-level max-age; {@code null} or non-positive = no TTL
   */
  public KeyValue ensureKv(String bucketName, Duration ttl)
      throws IOException, JetStreamApiException {
    KeyValue cached = kvCache.get(bucketName);
    if (cached != null) {
      return cached;
    }
    Object lock = kvLocks.computeIfAbsent(bucketName, k -> new Object());
    synchronized (lock) {
      cached = kvCache.get(bucketName);
      if (cached != null) {
        return cached;
      }
      try {
        KeyValueStatus status = kvm.getStatus(bucketName);
        if (status != null) {
          KeyValue kv = connection.keyValue(bucketName);
          kvCache.put(bucketName, kv);
          return kv;
        }
      } catch (JetStreamApiException missing) {
        // bucket absent — fall through to create
      }
      RqueueNatsConfig.StreamDefaults sd = config.getStreamDefaults();
      KeyValueConfiguration.Builder cfg = KeyValueConfiguration.builder()
          .name(bucketName)
          .compression(true)
          .replicas(sd.getReplicas())
          .storageType(sd.getStorage());
      if (ttl != null && !ttl.isZero() && !ttl.isNegative()) {
        cfg.ttl(ttl);
      }
      kvm.create(cfg.build());
      KeyValue kv = connection.keyValue(bucketName);
      kvCache.put(bucketName, kv);
      return kv;
    }
  }

  // ---- Stream provisioning ----------------------------------------------

  /**
   * Ensure a JetStream stream exists with the given subjects, using {@link QueueType#QUEUE}
   * (WorkQueue retention) as the default. Callers that have a {@link QueueType} available should
   * use {@link #ensureStream(String, List, QueueType)} instead.
   */
  public void ensureStream(String streamName, List<String> subjects) {
    ensureStream(streamName, subjects, QueueType.QUEUE, null, false);
  }

  /** See {@link #ensureStream(String, List, QueueType, String, boolean)}. */
  public void ensureStream(String streamName, List<String> subjects, QueueType queueType) {
    ensureStream(streamName, subjects, queueType, null, false);
  }

  /** See {@link #ensureStream(String, List, QueueType, String, boolean)}. */
  public void ensureStream(
      String streamName, List<String> subjects, QueueType queueType, String description) {
    ensureStream(streamName, subjects, queueType, description, false);
  }

  /**
   * Ensure a JetStream stream exists with the given subjects and retention policy derived from
   * {@code queueType}:
   * <ul>
   *   <li>{@link QueueType#QUEUE} — {@link io.nats.client.api.RetentionPolicy#WorkQueue}: each
   *       message is delivered to exactly one consumer; competing-consumer semantics.
   *   <li>{@link QueueType#STREAM} — {@link io.nats.client.api.RetentionPolicy#Limits}: every
   *       independent durable consumer group receives all messages; stream/fan-out semantics.
   * </ul>
   *
   * <p>{@code description} is forwarded to JetStream as the stream's description (visible via
   * {@code nats stream info}). Callers should pass the rqueue queue name so operators can map a
   * stream back to the queue that created it; pass {@code null} to skip.
   *
   * <p>{@code allowSchedules} must be {@code true} when the stream will receive messages published
   * with the {@code Nats-Schedule} header (ADR-51). Only callers that perform delayed
   * enqueue should pass {@code true}; regular enqueue callers should pass {@code false} (or use the
   * shorter overloads). Equivalent to the CLI flag {@code --allow-schedules}.
   *
   * <p>Hits the NATS backend at most once per stream name per process lifetime; subsequent calls
   * return immediately from the in-process cache. If {@code allowSchedules=true} is later requested
   * for a stream that was previously created without that flag, the stream is updated in place via
   * {@link JetStreamManagement#updateStream}. If the stream already exists with a different
   * retention policy, a WARNING is logged and the existing config is left untouched.
   */
  public void ensureStream(
      String streamName,
      List<String> subjects,
      QueueType queueType,
      String description,
      boolean allowSchedules) {
    // Fast-path: already provisioned with at least as many capabilities as requested.
    Boolean cached = streamsDone.get(streamName);
    if (cached != null && (cached || !allowSchedules)) {
      return;
    }
    Object lock = streamLocks.computeIfAbsent(streamName, k -> new Object());
    synchronized (lock) {
      cached = streamsDone.get(streamName);
      if (cached != null && (cached || !allowSchedules)) {
        return;
      }
      boolean enableSchedules = allowSchedules && schedulingSupported;
      try {
        StreamInfo existing = safeGetStreamInfo(streamName);
        RetentionPolicy desired =
            queueType == QueueType.STREAM ? RetentionPolicy.Limits : RetentionPolicy.WorkQueue;
        if (existing == null) {
          if (!config.isAutoCreateStreams()) {
            throw new RqueueNatsException(
                "Stream '" + streamName + "' does not exist and autoCreateStreams=false");
          }
          RqueueNatsConfig.StreamDefaults sd = config.getStreamDefaults();
          StreamConfiguration.Builder b = StreamConfiguration.builder()
              .name(streamName)
              .subjects(subjects)
              .replicas(sd.getReplicas())
              .storageType(sd.getStorage())
              .retentionPolicy(desired)
              .compressionOption(CompressionOption.S2);
          if (enableSchedules) {
            // Enable server-side message scheduling (ADR-51 / Nats-Schedule header).
            // Equivalent to: nats stream add MY_STREAM --allow-schedules
            b.allowMessageSchedules(true);
          }
          if (description != null && !description.isEmpty()) {
            b.description(description);
          }
          if (sd.getMaxMsgs() > 0) {
            b.maxMessages(sd.getMaxMsgs());
          }
          if (sd.getMaxBytes() > 0) {
            b.maxBytes(sd.getMaxBytes());
          }
          if (sd.getMaxAge() != null
              && !sd.getMaxAge().isZero()
              && !sd.getMaxAge().isNegative()) {
            b.maxAge(sd.getMaxAge());
          }
          jsm.addStream(b.build());
        } else {
          RetentionPolicy actual = existing.getConfiguration().getRetentionPolicy();
          if (actual != desired) {
            log.log(
                Level.WARNING,
                "Stream ''{0}'' exists with retention={1} but queueMode requires retention={2}"
                    + " — leaving existing config in place.",
                new Object[] {streamName, actual, desired});
          }
          // Check whether new subjects need to be merged in (e.g. the sched wildcard added by
          // enqueueWithDelay after the stream was originally created by a plain enqueue call).
          java.util.List<String> existingSubjects = existing.getConfiguration().getSubjects();
          java.util.Set<String> existingSet = existingSubjects != null
              ? new java.util.HashSet<>(existingSubjects) : new java.util.HashSet<>();
          boolean needsSubjectUpdate = subjects.stream().anyMatch(s -> !existingSet.contains(s));
          boolean needsFlagUpdate =
              enableSchedules && !existing.getConfiguration().getAllowMsgSchedules();

          if (needsFlagUpdate || needsSubjectUpdate) {
            // Merge: keep all existing subjects and append new ones (never remove).
            java.util.LinkedHashSet<String> merged = new java.util.LinkedHashSet<>(existingSet);
            merged.addAll(subjects);
            StreamConfiguration.Builder upd =
                StreamConfiguration.builder(existing.getConfiguration())
                    .subjects(new java.util.ArrayList<>(merged));
            if (needsFlagUpdate) {
              upd.allowMessageSchedules(true);
            }
            jsm.updateStream(upd.build());
            if (needsFlagUpdate) {
              log.log(Level.INFO,
                  "Stream ''{0}'' updated to enable message scheduling (ADR-51).", streamName);
            }
            if (needsSubjectUpdate) {
              log.log(Level.INFO,
                  "Stream ''{0}'' updated with additional subjects: {1}.",
                  new Object[] {streamName, subjects});
            }
          }
        }
      } catch (IOException | JetStreamApiException e) {
        throw new RqueueNatsException(
            "Failed to ensure stream '" + streamName + "' for subjects " + subjects, e);
      }
      streamsDone.put(streamName, enableSchedules);
    }
  }

  /**
   * Ensure a durable pull consumer exists, returning the consumer name.
   * Hits the NATS backend at most once per (stream, consumer) pair per process lifetime.
   *
   * <p>Overload without a filter subject: used when the stream has only the work subject so the
   * filter would be redundant, or when multiple independent consumer groups (fan-out) must coexist
   * on a Limits-retention stream (NATS rejects two consumers with the same filter subject, error
   * 10100). For WorkQueue streams that also carry scheduler subjects ({@code .sched.*}) a filter
   * subject MUST be supplied via
   * {@link #ensureConsumer(String, String, String, Duration, long, long)} so the consumer only
   * receives work-subject messages and does not accidentally pick up scheduler entries.
   */
  public String ensureConsumer(
      String streamName,
      String consumerName,
      Duration ackWait,
      long maxDeliver,
      long maxAckPending) {
    return ensureConsumer(streamName, consumerName, null, ackWait, maxDeliver, maxAckPending);
  }

  /**
   * Ensure a durable pull consumer exists with an optional subject filter, returning the consumer
   * name. Hits the NATS backend at most once per (stream, consumer) pair per process lifetime.
   *
   * <p>{@code filterSubject} — when non-null, sets the consumer's filter subject so it only
   * receives messages published to that subject. Required when the stream carries both work subjects
   * and scheduler subjects ({@code .sched.*}): without a filter the consumer reads scheduler
   * entries before the scheduled time, delivering the message early. Pass {@code null} for streams
   * that do not use NATS scheduling, or where fan-out across multiple consumers is needed (Limits
   * retention).
   */
  public String ensureConsumer(
      String streamName,
      String consumerName,
      String filterSubject,
      Duration ackWait,
      long maxDeliver,
      long maxAckPending) {
    String cacheKey = streamName + "/" + consumerName;
    String cached = consumerCache.get(cacheKey);
    if (cached != null) {
      return cached;
    }
    Object lock = consumerLocks.computeIfAbsent(cacheKey, k -> new Object());
    synchronized (lock) {
      cached = consumerCache.get(cacheKey);
      if (cached != null) {
        return cached;
      }
      String actual = doEnsureConsumer(
          streamName, consumerName, filterSubject, ackWait, maxDeliver, maxAckPending);
      consumerCache.put(cacheKey, actual);
      return actual;
    }
  }

  private String doEnsureConsumer(
      String streamName,
      String consumerName,
      String filterSubject,
      Duration ackWait,
      long maxDeliver,
      long maxAckPending) {
    try {
      ConsumerInfo info = safeGetConsumerInfo(streamName, consumerName);
      if (info != null) {
        ConsumerConfiguration cc = info.getConsumerConfiguration();
        if (cc.getAckWait() != null && !cc.getAckWait().equals(ackWait)) {
          log.log(
              Level.WARNING,
              "Consumer " + streamName + "/" + consumerName
                  + " ackWait differs (existing=" + cc.getAckWait()
                  + ", desired=" + ackWait + ") - leaving existing config in place.");
        }
        if (cc.getMaxDeliver() != maxDeliver) {
          log.log(
              Level.WARNING,
              "Consumer " + streamName + "/" + consumerName
                  + " maxDeliver differs (existing=" + cc.getMaxDeliver()
                  + ", desired=" + maxDeliver + ") - leaving existing config in place.");
        }
        return consumerName;
      }
      if (!config.isAutoCreateConsumers()) {
        throw new RqueueNatsException("Consumer '" + consumerName + "' on stream '" + streamName
            + "' does not exist and autoCreateConsumers=false");
      }
      ConsumerConfiguration.Builder ccBuilder = ConsumerConfiguration.builder()
          .durable(consumerName)
          .ackPolicy(AckPolicy.Explicit)
          .deliverPolicy(DeliverPolicy.All)
          .ackWait(ackWait)
          .maxDeliver(maxDeliver)
          .maxAckPending(maxAckPending);
      if (filterSubject != null && !filterSubject.isEmpty()) {
        // Filter to the work subject only so that scheduler entries (published to
        // <workSubject>.sched.*) are not delivered to this consumer before the scheduled time.
        // The NATS scheduler fires the triggered message to workSubject when the time arrives.
        ccBuilder.filterSubject(filterSubject);
      }
      jsm.addOrUpdateConsumer(streamName, ccBuilder.build());
      return consumerName;
    } catch (JetStreamApiException e) {
      throw new RqueueNatsException(
          "Failed to ensure consumer '" + consumerName + "' on stream '" + streamName + "'", e);
    } catch (IOException e) {
      throw new RqueueNatsException(
          "Failed to ensure consumer '" + consumerName + "' on stream '" + streamName + "'", e);
    }
  }

  /** Ensure a DLQ stream exists capturing dead-letter subjects (e.g. "rqueue.js.*.dlq"). */
  public void ensureDlqStream(String dlqStreamName, List<String> dlqSubjects) {
    if (!config.isAutoCreateDlqStream()) {
      return;
    }
    ensureStream(dlqStreamName, dlqSubjects);
  }

  // ---- private helpers --------------------------------------------------

  private StreamInfo safeGetStreamInfo(String streamName)
      throws IOException, JetStreamApiException {
    try {
      return jsm.getStreamInfo(streamName);
    } catch (JetStreamApiException e) {
      // 10059 = stream not found
      if (e.getApiErrorCode() == 10059 || e.getErrorCode() == 404) {
        return null;
      }
      throw e;
    }
  }

  private ConsumerInfo safeGetConsumerInfo(String streamName, String consumerName)
      throws IOException, JetStreamApiException {
    try {
      return jsm.getConsumerInfo(streamName, consumerName);
    } catch (JetStreamApiException e) {
      // 10014 = consumer not found, 10059 = stream not found
      if (e.getApiErrorCode() == 10014 || e.getApiErrorCode() == 10059 || e.getErrorCode() == 404) {
        return null;
      }
      throw e;
    }
  }
}
