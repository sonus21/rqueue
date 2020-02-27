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

import static org.junit.Assert.assertEquals;

import com.github.sonus21.rqueue.metrics.RqueueMetricsPropertiesTest.MetricProperties;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tags;
import io.micrometer.core.instrument.search.MeterNotFoundException;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.StrictStubs.class)
public class QueueCounterTest {
  private String simpleQueue = "simple-queue";
  private String delayedQueue = "delayed-queue";
  private MetricProperties metricsProperties = new MetricProperties();

  private void updateCount(String type, QueueCounter q, String queueName) {
    if (type.equals("failure")) {
      q.updateFailureCount(queueName);
    } else {
      q.updateExecutionCount(queueName);
    }
  }

  private void registerQueue(
      MetricProperties metricsProperties,
      MeterRegistry meterRegistry,
      String queueName,
      String type) {
    QueueCounter q = new QueueCounter();
    q.registerQueue(metricsProperties, Tags.of("queue", queueName), meterRegistry, queueName);
    updateCount(type, q, queueName);
    updateCount(type, q, queueName + queueName);
  }

  private void validateCountStatistics(String queueName, String type) {
    String dataName = "execution.count";
    if (type.equals("failure")) {
      dataName = "failure.count";
    }
    MeterRegistry meterRegistry = new SimpleMeterRegistry();
    registerQueue(metricsProperties, meterRegistry, queueName, type);
    try {
      meterRegistry.get(dataName).tags(Tags.of("queue", queueName)).counter().count();
      Assert.fail();
    } catch (MeterNotFoundException e) {
    }
    meterRegistry = new SimpleMeterRegistry();
    if (type.equals("failure")) {
      metricsProperties.getCount().setFailure(true);
    } else {
      metricsProperties.getCount().setExecution(true);
    }
    registerQueue(metricsProperties, meterRegistry, queueName, type);
    assertEquals(
        1, meterRegistry.get(dataName).tags(Tags.of("queue", queueName)).counter().count(), 0);
  }

  @Test
  public void updateFailureCount() {
    validateCountStatistics(simpleQueue, "failure");
  }

  @Test
  public void updateExecutionCount() {
    validateCountStatistics(delayedQueue, "success");
  }
}
