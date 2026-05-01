/*
 * Copyright (c) 2024-2026 Sonu Kumar
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * You may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 */
package com.github.sonus21.rqueue.nats.internal;

import com.github.sonus21.rqueue.nats.RqueueNatsConfig;
import com.github.sonus21.rqueue.nats.RqueueNatsException;
import io.nats.client.JetStreamApiException;
import io.nats.client.JetStreamManagement;
import io.nats.client.api.AckPolicy;
import io.nats.client.api.ConsumerConfiguration;
import io.nats.client.api.ConsumerInfo;
import io.nats.client.api.DeliverPolicy;
import io.nats.client.api.StreamConfiguration;
import io.nats.client.api.StreamInfo;
import java.io.IOException;
import java.time.Duration;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Idempotent stream/consumer provisioning helpers. All methods are safe to call repeatedly; if the
 * target object already exists with a compatible config, this class leaves it alone. If it exists
 * but with diverging config (e.g. ackWait, maxDeliver), this class logs a WARN and does NOT
 * auto-mutate so user customizations are preserved.
 */
public class NatsProvisioner {

  private static final Logger log = Logger.getLogger(NatsProvisioner.class.getName());

  private final JetStreamManagement jsm;
  private final RqueueNatsConfig config;

  public NatsProvisioner(JetStreamManagement jsm, RqueueNatsConfig config) {
    this.jsm = jsm;
    this.config = config;
  }

  /**
   * Ensure a JetStream stream exists with the given subjects. If absent and {@code
   * autoCreateStreams=true}, creates one using {@link RqueueNatsConfig.StreamDefaults}.
   */
  public void ensureStream(String streamName, List<String> subjects) {
    try {
      StreamInfo existing = safeGetStreamInfo(streamName);
      if (existing != null) {
        return;
      }
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
          .retentionPolicy(sd.getRetention())
          .duplicateWindow(sd.getDuplicateWindow());
      if (sd.getMaxMsgs() > 0) {
        b.maxMessages(sd.getMaxMsgs());
      }
      if (sd.getMaxBytes() > 0) {
        b.maxBytes(sd.getMaxBytes());
      }
      jsm.addStream(b.build());
    } catch (IOException | JetStreamApiException e) {
      throw new RqueueNatsException(
          "Failed to ensure stream '" + streamName + "' for subjects " + subjects, e);
    }
  }

  /**
   * Ensure a durable pull consumer exists. If existing config differs from desired, logs WARN and
   * leaves it alone (so users can hand-tune consumers in production).
   */
  public void ensureConsumer(
      String streamName,
      String consumerName,
      Duration ackWait,
      long maxDeliver,
      long maxAckPending,
      String filterSubject) {
    try {
      ConsumerInfo info = safeGetConsumerInfo(streamName, consumerName);
      if (info != null) {
        ConsumerConfiguration cc = info.getConsumerConfiguration();
        if (cc.getAckWait() != null && !cc.getAckWait().equals(ackWait)) {
          log.log(
              Level.WARNING,
              "Consumer "
                  + streamName
                  + "/"
                  + consumerName
                  + " ackWait differs (existing="
                  + cc.getAckWait()
                  + ", desired="
                  + ackWait
                  + ") - leaving existing config in place.");
        }
        if (cc.getMaxDeliver() != maxDeliver) {
          log.log(
              Level.WARNING,
              "Consumer "
                  + streamName
                  + "/"
                  + consumerName
                  + " maxDeliver differs (existing="
                  + cc.getMaxDeliver()
                  + ", desired="
                  + maxDeliver
                  + ") - leaving existing config in place.");
        }
        return;
      }
      if (!config.isAutoCreateConsumers()) {
        throw new RqueueNatsException("Consumer '"
            + consumerName
            + "' on stream '"
            + streamName
            + "' does not exist and autoCreateConsumers=false");
      }
      ConsumerConfiguration.Builder cb = ConsumerConfiguration.builder()
          .durable(consumerName)
          .ackPolicy(AckPolicy.Explicit)
          .deliverPolicy(DeliverPolicy.All)
          .ackWait(ackWait)
          .maxDeliver(maxDeliver)
          .maxAckPending(maxAckPending);
      if (filterSubject != null) {
        cb.filterSubject(filterSubject);
      }
      jsm.addOrUpdateConsumer(streamName, cb.build());
    } catch (IOException | JetStreamApiException e) {
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
      if (e.getApiErrorCode() == 10014 || e.getErrorCode() == 404) {
        return null;
      }
      throw e;
    }
  }
}
