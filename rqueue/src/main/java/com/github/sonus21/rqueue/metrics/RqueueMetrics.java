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

import com.github.sonus21.rqueue.common.RqueueRedisTemplate;
import com.github.sonus21.rqueue.config.MetricsProperties;
import com.github.sonus21.rqueue.listener.QueueDetail;
import com.github.sonus21.rqueue.models.event.QueueInitializationEvent;
import com.github.sonus21.rqueue.utils.QueueUtils;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.Gauge.Builder;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tags;
import java.util.Map;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationListener;
import org.springframework.scheduling.annotation.Async;

/**
 * RqueueMetrics register metrics related to queue. A queue can have 4 types of metrics like
 * queue.size, processing.queue.size and other two depends on the queue configurations. For delayed
 * queue messages can be in delayed queue because time has not reached. Some messages can be in dead
 * letter queue if dead letter queue is configured.
 */
public class RqueueMetrics implements ApplicationListener<QueueInitializationEvent> {
  private static final String QUEUE_SIZE = "queue.size";
  private static final String DELAYED_QUEUE_SIZE = "delayed.queue.size";
  private static final String PROCESSING_QUEUE_SIZE = "processing.queue.size";
  private static final String DEAD_LETTER_QUEUE_SIZE = "dead.letter.queue.size";
  private RqueueRedisTemplate<String> rqueueMessageTemplate;
  @Autowired private MetricsProperties metricsProperties;
  @Autowired private MeterRegistry meterRegistry;
  private QueueCounter queueCounter;

  public RqueueMetrics(
      RqueueRedisTemplate<String> rqueueMessageTemplate, QueueCounter queueCounter) {
    this.rqueueMessageTemplate = rqueueMessageTemplate;
    this.queueCounter = queueCounter;
  }

  private long size(String name, boolean isZset) {
    Long val;
    if (!isZset) {
      val = rqueueMessageTemplate.getListSize(name);
    } else {
      val = rqueueMessageTemplate.getZsetSize(name);
    }
    if (val == null || val < 0) {
      return 0;
    }
    return val;
  }

  private void monitor(Map<String, QueueDetail> queueDetailMap) {
    for (QueueDetail queueDetail : queueDetailMap.values()) {
      Tags queueTags =
          Tags.concat(metricsProperties.getMetricTags(), "queue", queueDetail.getQueueName());
      Gauge.builder(QUEUE_SIZE, queueDetail, c -> size(queueDetail.getQueueName(), false))
          .tags(queueTags)
          .description("The number of entries in this queue")
          .register(meterRegistry);
      Gauge.builder(
              PROCESSING_QUEUE_SIZE,
              queueDetail,
              c -> size(QueueUtils.getProcessingQueueName(queueDetail.getQueueName()), true))
          .tags(queueTags)
          .description("The number of entries in the processing queue")
          .register(meterRegistry);

      if (queueDetail.isDelayedQueue()) {
        Gauge.builder(
                DELAYED_QUEUE_SIZE,
                queueDetail,
                c -> size(QueueUtils.getDelayedQueueName(queueDetail.getQueueName()), true))
            .tags(queueTags)
            .description("The number of entries waiting in the delayed queue")
            .register(meterRegistry);
      }
      if (queueDetail.isDlqSet()) {
        Builder<QueueDetail> builder =
            Gauge.builder(
                DEAD_LETTER_QUEUE_SIZE,
                queueDetail,
                c -> size(queueDetail.getDeadLetterQueueName(), false));
        builder.tags(queueTags);
        builder.description("The number of entries in the dead letter queue");
        builder.register(meterRegistry);
      }
      queueCounter.registerQueue(
          metricsProperties, queueTags, meterRegistry, queueDetail.getQueueName());
    }
  }

  @Override
  @Async
  public void onApplicationEvent(QueueInitializationEvent event) {
    if (event.isStart()) {
      monitor(event.getQueueDetailMap());
    }
  }

  public QueueCounter getQueueCounter() {
    return this.queueCounter;
  }
}
