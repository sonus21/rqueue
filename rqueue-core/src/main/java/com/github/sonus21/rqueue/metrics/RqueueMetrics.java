/*
 * Copyright (c) 2019-2026 Sonu Kumar
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

package com.github.sonus21.rqueue.metrics;

import com.github.sonus21.rqueue.config.MetricsProperties;
import com.github.sonus21.rqueue.core.EndpointRegistry;
import com.github.sonus21.rqueue.listener.QueueDetail;
import com.github.sonus21.rqueue.models.event.RqueueBootstrapEvent;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.Gauge.Builder;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tags;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Async;

/**
 * RqueueMetrics register metrics related to queue. A queue can have 4 types of metrics like
 * queue.size, processing.queue.size and scheduled.queue.size. Some messages can be in dead letter
 * queue if dead letter queue is configured.
 */
public class RqueueMetrics implements RqueueMetricsRegistry {

  static final String QUEUE_KEY = "key";
  /**
   * Tag added when a {@link QueueDetail} declares a {@code consumerName} override. Without this
   * tag, two {@code @RqueueListener} methods on the same queue with different consumer names
   * register gauges with identical (name, tag-set) pairs and Micrometer silently keeps only the
   * first — losing the second consumer's metrics entirely.
   */
  static final String CONSUMER_KEY = "consumer";
  private static final String QUEUE_SIZE = "queue.size";
  private static final String SCHEDULED_QUEUE_SIZE = "scheduled.queue.size";
  private static final String PROCESSING_QUEUE_SIZE = "processing.queue.size";
  private static final String DEAD_LETTER_QUEUE_SIZE = "dead.letter.queue.size";
  private final QueueCounter queueCounter;

  @Autowired
  private MetricsProperties metricsProperties;

  @Autowired
  private MeterRegistry meterRegistry;

  @Autowired
  private RqueueQueueMetricsProvider queueMetricsProvider;

  public RqueueMetrics(QueueCounter queueCounter) {
    this.queueCounter = queueCounter;
  }

  private void monitor() {
    for (QueueDetail queueDetail : EndpointRegistry.getActiveQueueDetails()) {
      Tags queueTags =
          Tags.concat(metricsProperties.getMetricTags(), "queue", queueDetail.getName());
      // When a queue carries multiple consumers (multiple @RqueueListener with distinct
      // consumerName overrides), each gets its own QueueDetail. Without a `consumer` tag the
      // gauges would share the same (name, tags) and Micrometer would drop all but the first.
      String consumerName = queueDetail.getConsumerName();
      boolean hasConsumerOverride = consumerName != null && !consumerName.isEmpty();
      if (hasConsumerOverride) {
        queueTags = queueTags.and(CONSUMER_KEY, consumerName);
      }
      Gauge.builder(
              metricsProperties.getMetricName(QUEUE_SIZE),
              queueDetail,
              c -> hasConsumerOverride
                  ? queueMetricsProvider.getPendingMessageCountByConsumer(
                      queueDetail.getName(), consumerName)
                  : queueMetricsProvider.getPendingMessageCount(queueDetail.getName()))
          .tags(queueTags.and(QUEUE_KEY, queueDetail.getQueueName()))
          .description("The number of entries in this queue")
          .register(meterRegistry);
      Gauge.builder(
              metricsProperties.getMetricName(PROCESSING_QUEUE_SIZE),
              queueDetail,
              c -> hasConsumerOverride
                  ? queueMetricsProvider.getProcessingMessageCountByConsumer(
                      queueDetail.getName(), consumerName)
                  : queueMetricsProvider.getProcessingMessageCount(queueDetail.getName()))
          .tags(queueTags.and(QUEUE_KEY, queueDetail.getProcessingQueueName()))
          .description("The number of entries in the processing queue")
          .register(meterRegistry);
      Gauge.builder(
              metricsProperties.getMetricName(SCHEDULED_QUEUE_SIZE),
              queueDetail,
              c -> queueMetricsProvider.getScheduledMessageCount(queueDetail.getName()))
          .tags(queueTags.and(QUEUE_KEY, queueDetail.getScheduledQueueName()))
          .description("The number of entries waiting in the scheduled queue")
          .register(meterRegistry);
      if (queueDetail.isDlqSet()) {
        Builder<QueueDetail> builder = Gauge.builder(
            metricsProperties.getMetricName(DEAD_LETTER_QUEUE_SIZE),
            queueDetail,
            c -> queueMetricsProvider.getDeadLetterMessageCount(queueDetail.getName()));
        builder.tags(queueTags);
        builder.description("The number of entries in the dead letter queue");
        builder.register(meterRegistry);
      }
      queueCounter.registerQueue(metricsProperties, queueTags, meterRegistry, queueDetail);
    }
  }

  @Override
  @Async
  public void onApplicationEvent(RqueueBootstrapEvent event) {
    if (event.isStartup()) {
      monitor();
    }
  }

  @Override
  public QueueCounter getQueueCounter() {
    return this.queueCounter;
  }
}
