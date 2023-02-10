/*
 * Copyright (c) 2019-2023 Sonu Kumar
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

import static com.github.sonus21.rqueue.metrics.QueueCounter.EXECUTION_COUNT;
import static com.github.sonus21.rqueue.metrics.QueueCounter.FAILURE_COUNT;
import static com.github.sonus21.rqueue.metrics.RqueueMetrics.QUEUE_KEY;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

import com.github.sonus21.TestBase;
import com.github.sonus21.rqueue.CoreUnitTest;
import com.github.sonus21.rqueue.listener.QueueDetail;
import com.github.sonus21.rqueue.metrics.MetricsPropertiesTest.MetricProperties;
import com.github.sonus21.rqueue.utils.TestUtils;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tags;
import io.micrometer.core.instrument.search.MeterNotFoundException;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.Test;

@CoreUnitTest
class QueueCounterTest extends TestBase {

  private final MetricProperties metricsProperties = new MetricProperties();

  private void updateCount(String type, QueueCounter counter, String queueName) {
    if (type.equals("failure")) {
      counter.updateFailureCount(queueName);
    } else {
      counter.updateExecutionCount(queueName);
    }
  }

  private void registerQueue(
      MetricProperties metricsProperties,
      MeterRegistry meterRegistry,
      QueueDetail queueDetail,
      String type) {
    QueueCounter counter = new QueueCounter();
    counter.registerQueue(
        metricsProperties, Tags.of("queue", queueDetail.getName()), meterRegistry, queueDetail);
    updateCount(type, counter, queueDetail.getName());
  }

  private void validateCountStatistics(QueueDetail queueDetail, String type) {
    String dataName = EXECUTION_COUNT;
    if (type.equals("failure")) {
      dataName = FAILURE_COUNT;
    }
    MeterRegistry meterRegistry = new SimpleMeterRegistry();
    registerQueue(metricsProperties, meterRegistry, queueDetail, type);
    Tags tags = Tags.of("queue", queueDetail.getName(), QUEUE_KEY, queueDetail.getQueueName());
    try {
      meterRegistry.get(dataName).tags(tags).counter();
      fail();
    } catch (MeterNotFoundException e) {
    }
    meterRegistry = new SimpleMeterRegistry();
    if (type.equals("failure")) {
      metricsProperties.getCount().setFailure(true);
    } else {
      metricsProperties.getCount().setExecution(true);
    }
    registerQueue(metricsProperties, meterRegistry, queueDetail, type);
    assertEquals(1, meterRegistry.get(dataName).tags(tags).counter().count(), 0);
  }

  @Test
  void updateFailureCount() {
    validateCountStatistics(TestUtils.createQueueDetail("simple-queue", 10000L), "failure");
  }

  @Test
  void updateExecutionCount() {
    validateCountStatistics(TestUtils.createQueueDetail("scheduled-queue", 900000L), "success");
  }
}
