/*
 * Copyright (c) 2019-2019, Sonu Kumar
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

import com.github.sonus21.rqueue.listener.ConsumerQueueDetail;
import com.github.sonus21.rqueue.listener.RqueueMessageListenerContainer;
import com.github.sonus21.rqueue.utils.QueueInfo;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.Gauge.Builder;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tags;
import io.micrometer.core.instrument.binder.MeterBinder;
import java.lang.ref.WeakReference;

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
  private RqueueCounter rqueueCounter;
  private WeakReference<RqueueMessageListenerContainer> container;
  private RqueueMetricsProperties metricsProperties;

  public RqueueMetrics(
      RqueueMessageListenerContainer container,
      RqueueMetricsProperties metricsProperties,
      RqueueCounter counter) {
    this.metricsProperties = metricsProperties;
    rqueueCounter = counter;
    this.container = new WeakReference<>(container);
  }

  public static RqueueMetrics monitor(
      RqueueMessageListenerContainer container,
      MeterRegistry registry,
      RqueueMetricsProperties metricsProperties,
      RqueueCounter counter) {
    if (container == null) {
      return null;
    }
    RqueueMetrics rqueueMetrics = new RqueueMetrics(container, metricsProperties, counter);
    rqueueMetrics.bindTo(registry);
    return rqueueMetrics;
  }

  private int numQueues() {
    RqueueMessageListenerContainer listenerContainer = container.get();
    if (listenerContainer == null) {
      return 0;
    }
    return listenerContainer.getRegisteredQueues().size();
  }

  private long size(String name, boolean isZset) {
    Long val;
    if (!isZset) {
      val = container.get().getRqueueMessageTemplate().getListLength(name);
    } else {
      val = container.get().getRqueueMessageTemplate().getZsetSize(name);
    }
    if (val == null || val < 0) {
      return 0;
    }
    return val;
  }

  @Override
  public void bindTo(MeterRegistry registry) {
    if (numQueues() == 0) {
      return;
    }
    for (ConsumerQueueDetail queueDetail : container.get().getRegisteredQueues().values()) {
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
      if (rqueueCounter != null) {
        rqueueCounter.registerQueue(
            metricsProperties, queueTags, registry, queueDetail.getQueueName());
      }
    }
  }
}
