/*
 * Copyright (c) 2024-2026 Sonu Kumar
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
import com.github.sonus21.rqueue.listener.QueueDetail;
import com.github.sonus21.rqueue.metrics.RqueueQueueMetricsProvider;
import com.github.sonus21.rqueue.nats.RqueueNatsConfig;
import com.github.sonus21.rqueue.nats.RqueueNatsException;
import io.nats.client.JetStreamApiException;
import io.nats.client.JetStreamManagement;
import io.nats.client.api.StreamInfo;
import java.io.IOException;

/**
 * NATS JetStream {@link RqueueQueueMetricsProvider}. Pending messages map to the message count of
 * the queue's stream ({@code <streamPrefix><queueName>}). NATS does not natively support delayed
 * delivery in this module, so {@link #getScheduledMessageCount(String)} always returns {@code 0}.
 *
 * <p>If the underlying stream has not yet been provisioned (no enqueue has happened), both methods
 * return {@code 0} rather than failing — callers use the values as gauge readings.
 */
public class NatsRqueueQueueMetricsProvider implements RqueueQueueMetricsProvider {

  private final JetStreamManagement jsm;
  private final RqueueNatsConfig config;

  public NatsRqueueQueueMetricsProvider(JetStreamManagement jsm, RqueueNatsConfig config) {
    this.jsm = jsm;
    this.config = config;
  }

  @Override
  public long getPendingMessageCount(String queueName) {
    QueueDetail q = EndpointRegistry.get(queueName);
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
    QueueDetail q = EndpointRegistry.get(queueName);
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
}
