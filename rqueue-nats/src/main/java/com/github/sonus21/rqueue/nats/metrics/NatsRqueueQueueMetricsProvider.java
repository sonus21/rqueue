/*
 * Copyright (c) 2026 Sonu Kumar
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * You may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and limitations under the License.
 *
 */
package com.github.sonus21.rqueue.nats.metrics;

import com.github.sonus21.rqueue.core.EndpointRegistry;
import com.github.sonus21.rqueue.exception.QueueDoesNotExist;
import com.github.sonus21.rqueue.listener.QueueDetail;
import com.github.sonus21.rqueue.metrics.RqueueQueueMetricsProvider;
import com.github.sonus21.rqueue.nats.RqueueNatsConfig;
import com.github.sonus21.rqueue.nats.RqueueNatsException;
import io.nats.client.JetStreamApiException;
import io.nats.client.JetStreamManagement;
import io.nats.client.api.ConsumerInfo;
import io.nats.client.api.StreamInfo;
import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * NATS JetStream {@link RqueueQueueMetricsProvider}. Pending messages map to the message count of
 * the queue's stream ({@code <streamPrefix><queueName>}). NATS does not natively support delayed
 * delivery in this module, so {@link #getScheduledMessageCount(String)} always returns {@code 0}.
 *
 * <p>If the underlying stream has not yet been provisioned (no enqueue has happened), both methods
 * return {@code 0} rather than failing — callers use the values as gauge readings.
 */
public class NatsRqueueQueueMetricsProvider implements RqueueQueueMetricsProvider {

  private static final Logger log =
      Logger.getLogger(NatsRqueueQueueMetricsProvider.class.getName());

  private final JetStreamManagement jsm;
  private final RqueueNatsConfig config;

  public NatsRqueueQueueMetricsProvider(JetStreamManagement jsm, RqueueNatsConfig config) {
    this.jsm = jsm;
    this.config = config;
  }

  @Override
  public long getPendingMessageCount(String queueName) {
    QueueDetail q;
    try {
      q = EndpointRegistry.get(queueName);
    } catch (QueueDoesNotExist e) {
      // unknown queue name -> 0 (mirrors how RedisRqueueQueueMetricsProvider handles it)
      return 0L;
    }
    String stream = config.getStreamPrefix() + q.getName();
    try {
      StreamInfo info = jsm.getStreamInfo(stream);
      return info.getStreamState().getMsgCount();
    } catch (JetStreamApiException e) {
      // stream not yet provisioned -> nothing pending
      return 0L;
    } catch (IOException e) {
      throw new RqueueNatsException(
          "Failed to read stream size for queue=" + queueName + " stream=" + stream, e);
    }
  }

  @Override
  public long getScheduledMessageCount(String queueName) {
    // JetStream backend does not support delayed enqueue; nothing is ever scheduled.
    return 0L;
  }

  @Override
  public long getProcessingMessageCount(String queueName) {
    // JetStream tracks in-flight messages on the consumer rather than as a separate queue. We
    // don't expose this depth in v1; report 0 to keep the gauge well-defined.
    return 0L;
  }

  @Override
  public long getDeadLetterMessageCount(String queueName) {
    QueueDetail q;
    try {
      q = EndpointRegistry.get(queueName);
    } catch (QueueDoesNotExist e) {
      return 0L;
    }
    String dlqStream = config.getStreamPrefix() + q.getName() + config.getDlqStreamSuffix();
    try {
      StreamInfo info = jsm.getStreamInfo(dlqStream);
      return info.getStreamState().getMsgCount();
    } catch (JetStreamApiException e) {
      // DLQ stream not provisioned -> nothing dead-lettered yet
      return 0L;
    } catch (IOException e) {
      throw new RqueueNatsException(
          "Failed to read DLQ stream size for queue=" + queueName + " stream=" + dlqStream, e);
    }
  }

  /**
   * Per-consumer pending depth: {@code ConsumerInfo.numPending}, the JetStream-tracked count of
   * messages this durable has yet to deliver. Falls back to the queue-level stream count when
   * {@code consumerName} is null/empty (no override) or when the consumer hasn't been provisioned
   * yet (boot-time race), so dashboards never see a missing reading.
   */
  @Override
  public long getPendingMessageCountByConsumer(String queueName, String consumerName) {
    if (consumerName == null || consumerName.isEmpty()) {
      return getPendingMessageCount(queueName);
    }
    QueueDetail q;
    try {
      q = EndpointRegistry.get(queueName);
    } catch (QueueDoesNotExist e) {
      return 0L;
    }
    String stream = config.getStreamPrefix() + q.getName();
    try {
      ConsumerInfo ci = jsm.getConsumerInfo(stream, consumerName);
      return ci == null ? 0L : ci.getNumPending();
    } catch (JetStreamApiException e) {
      // consumer or stream not yet provisioned — fall back to stream-level count rather than 0,
      // so the gauge reads the same as the bare-queue overload during the bootstrap window.
      log.log(
          Level.FINE,
          "Consumer-aware pending lookup fell back to stream count: queue=" + queueName
              + " consumer=" + consumerName + " (" + e.getMessage() + ")");
      return getPendingMessageCount(queueName);
    } catch (IOException e) {
      throw new RqueueNatsException(
          "Failed to read consumer info for queue=" + queueName + " consumer=" + consumerName
              + " stream=" + stream,
          e);
    }
  }

  /**
   * Per-consumer in-flight depth: {@code ConsumerInfo.numAckPending}, the count of messages
   * delivered to this consumer but not yet acked. This is the JetStream-side analog of the
   * Redis processing ZSET; reporting it per-consumer is essential when multiple
   * {@code @RqueueListener} methods on the same queue progress at different rates.
   */
  @Override
  public long getProcessingMessageCountByConsumer(String queueName, String consumerName) {
    if (consumerName == null || consumerName.isEmpty()) {
      return getProcessingMessageCount(queueName);
    }
    QueueDetail q;
    try {
      q = EndpointRegistry.get(queueName);
    } catch (QueueDoesNotExist e) {
      return 0L;
    }
    String stream = config.getStreamPrefix() + q.getName();
    try {
      ConsumerInfo ci = jsm.getConsumerInfo(stream, consumerName);
      return ci == null ? 0L : ci.getNumAckPending();
    } catch (JetStreamApiException e) {
      log.log(
          Level.FINE,
          "Consumer-aware processing lookup fell back to 0: queue=" + queueName + " consumer="
              + consumerName + " (" + e.getMessage() + ")");
      return 0L;
    } catch (IOException e) {
      throw new RqueueNatsException(
          "Failed to read consumer info for queue=" + queueName + " consumer=" + consumerName
              + " stream=" + stream,
          e);
    }
  }
}
