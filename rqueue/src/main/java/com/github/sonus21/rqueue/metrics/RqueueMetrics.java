/*
 * Copyright 2020 Sonu Kumar
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.github.sonus21.rqueue.metrics;

import com.github.sonus21.rqueue.core.RqueueMessageTemplate;
import com.github.sonus21.rqueue.listener.ConsumerQueueDetail;
import com.github.sonus21.rqueue.listener.RqueueMessageListenerContainer;
import com.github.sonus21.rqueue.utils.QueueInfo;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.Gauge.Builder;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tags;
import io.micrometer.core.instrument.binder.MeterBinder;
import java.util.Map;
import org.springframework.data.util.Pair;

/**
 * RqueueMetrics register metrics related to queue. A queue can have 4 types of metrics like
 * queue.size, processing.queue.size and other two depends on the queue configurations. For delayed
 * queue messages can be in delayed queue because time has not reached. Some messages can be in dead
 * letter queue if dead letter queue is configured.
 */
public class RqueueMetrics implements MeterBinder {
  private static final String QUEUE_SIZE = "queue.size";
  private static final String DELAYED_QUEUE_SIZE = "delayed.queue.size";
  private static final String PROCESSING_QUEUE_SIZE = "processing.queue.size";
  private static final String DEAD_LETTER_QUEUE_SIZE = "dead.letter.queue.size";
  private QueueCounter queueCounter;
  private RqueueMessageTemplate rqueueMessageTemplate;
  private RqueueMetricsProperties metricsProperties;
  private Map<String, ConsumerQueueDetail> queueDetailMap;

  private RqueueMetrics(
      RqueueMessageTemplate rqueueMessageTemplate,
      RqueueMetricsProperties metricsProperties,
      QueueCounter counter,
      Map<String, ConsumerQueueDetail> queueDetailMap) {
    this.metricsProperties = metricsProperties;
    this.queueCounter = counter;
    this.rqueueMessageTemplate = rqueueMessageTemplate;
    this.queueDetailMap = queueDetailMap;
  }

  static Pair<RqueueMetrics, RqueueCounter> monitor(
      RqueueMessageListenerContainer container,
      MeterRegistry registry,
      RqueueMetricsProperties metricsProperties,
      QueueCounter queueCounter) {
    RqueueMetrics rqueueMetrics =
        new RqueueMetrics(
            container.getRqueueMessageTemplate(),
            metricsProperties,
            queueCounter,
            container.getRegisteredQueues());
    rqueueMetrics.bindTo(registry);
    return Pair.of(rqueueMetrics, new RqueueCounter(queueCounter));
  }

  public static Pair<RqueueMetrics, RqueueCounter> monitor(
      RqueueMessageListenerContainer container,
      MeterRegistry registry,
      RqueueMetricsProperties metricsProperties) {
    return monitor(container, registry, metricsProperties, new QueueCounter());
  }

  private long size(String name, boolean isZset) {
    Long val;
    if (!isZset) {
      val = rqueueMessageTemplate.getListLength(name);
    } else {
      val = rqueueMessageTemplate.getZsetSize(name);
    }
    if (val == null || val < 0) {
      return 0;
    }
    return val;
  }

  @Override
  public void bindTo(MeterRegistry registry) {
    for (ConsumerQueueDetail queueDetail : queueDetailMap.values()) {
      Tags queueTags =
          Tags.concat(metricsProperties.getMetricTags(), "queue", queueDetail.getQueueName());
      Gauge.builder(QUEUE_SIZE, queueDetail, c -> size(queueDetail.getQueueName(), false))
          .tags(queueTags)
          .description("The number of entries in this queue")
          .register(registry);
      Gauge.builder(
              PROCESSING_QUEUE_SIZE,
              queueDetail,
              c -> size(QueueInfo.getProcessingQueueName(queueDetail.getQueueName()), true))
          .tags(queueTags)
          .description("The number of entries in the processing queue")
          .register(registry);

      if (queueDetail.isDelayedQueue()) {
        Gauge.builder(
                DELAYED_QUEUE_SIZE,
                queueDetail,
                c -> size(QueueInfo.getTimeQueueName(queueDetail.getQueueName()), true))
            .tags(queueTags)
            .description("The number of entries waiting in the delayed queue")
            .register(registry);
      }
      if (queueDetail.isDlqSet()) {
        Builder<ConsumerQueueDetail> builder =
            Gauge.builder(
                DEAD_LETTER_QUEUE_SIZE, queueDetail, c -> size(queueDetail.getDlqName(), false));
        builder.tags(queueTags);
        builder.description("The number of entries in the dead letter queue");
        builder.register(registry);
      }
      queueCounter.registerQueue(
          metricsProperties, queueTags, registry, queueDetail.getQueueName());
    }
  }
}
