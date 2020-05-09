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
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import com.github.sonus21.rqueue.common.RqueueRedisTemplate;
import com.github.sonus21.rqueue.config.MetricsProperties;
import com.github.sonus21.rqueue.core.QueueRegistry;
import com.github.sonus21.rqueue.listener.QueueDetail;
import com.github.sonus21.rqueue.models.event.RqueueBootstrapEvent;
import com.github.sonus21.rqueue.utils.TestUtils;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tags;
import io.micrometer.core.instrument.search.MeterNotFoundException;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.StrictStubs.class)
public class RqueueMetricsTest {
  private RqueueRedisTemplate<String> template = mock(RqueueRedisTemplate.class);
  private MetricsProperties metricsProperties = new MetricsProperties() {};
  private QueueCounter queueCounter = mock(QueueCounter.class);
  private String simpleQueue = "simple-queue";
  private String delayedQueue = "delayed-queue";
  private String deadLetterQueue = "dlq";
  private Tags tags = Tags.of("rQueue", "dc1");
  private QueueDetail simpleQueueDetail = TestUtils.createQueueDetail(simpleQueue, deadLetterQueue);
  private QueueDetail delayedQueueDetail = TestUtils.createQueueDetail(delayedQueue);

  @Before
  public void init() {
    QueueRegistry.delete();
    QueueRegistry.register(simpleQueueDetail);
    QueueRegistry.register(delayedQueueDetail);
    doAnswer(
            invocation -> {
              String zsetName = invocation.getArgument(0);
              if (zsetName.equals(delayedQueueDetail.getDelayedQueueName())) {
                return 5L;
              }
              if (zsetName.equals(simpleQueueDetail.getProcessingQueueName())) {
                return 10L;
              }
              if (zsetName.equals(delayedQueueDetail.getProcessingQueueName())) {
                return 15L;
              }
              return null;
            })
        .when(template)
        .getZsetSize(anyString());

    doAnswer(
            invocation -> {
              String listName = invocation.getArgument(0);
              if (listName.equals(simpleQueueDetail.getQueueName())) {
                return 100L;
              }
              if (listName.equals(delayedQueueDetail.getQueueName())) {
                return 200L;
              }
              if (listName.equals(deadLetterQueue)) {
                return 300L;
              }
              return null;
            })
        .when(template)
        .getListSize(anyString());
  }

  private void verifyQueueStatistics(
      MeterRegistry registry,
      String name,
      double queueSize,
      long processingQueueSize,
      long deadLetterQueueCount,
      long delayedQueueSize) {
    Tags tags = Tags.of("queue", name);
    assertEquals(queueSize, registry.get("queue.size").tags(tags).gauge().value(), 0);
    assertEquals(
        processingQueueSize, registry.get("processing.queue.size").tags(tags).gauge().value(), 0);
    try {
      double val = registry.get("dead.letter.queue.size").tags(tags).gauge().value();
      assertEquals(deadLetterQueueCount, val, 0);
    } catch (MeterNotFoundException e) {
      assertEquals(0, deadLetterQueueCount);
    }
    try {
      double val = registry.get("delayed.queue.size").tags(tags).gauge().value();
      assertEquals(delayedQueueSize, val, 0);
    } catch (MeterNotFoundException e) {
      assertEquals(0, delayedQueueSize);
    }
  }

  private void verifyCounterRegisterMethodIsCalled(Tags tags) throws IllegalAccessException {
    MeterRegistry meterRegistry = new SimpleMeterRegistry();
    metricsProperties.setMetricTags(tags);
    RqueueMetrics metrics = rqueueMetrics(meterRegistry, metricsProperties);
    metrics.onApplicationEvent(new RqueueBootstrapEvent("Test", true));
    verify(queueCounter, times(1))
        .registerQueue(
            metricsProperties,
            Tags.concat(tags, "queue", simpleQueue),
            meterRegistry,
            simpleQueueDetail);
    verify(queueCounter, times(1))
        .registerQueue(
            metricsProperties,
            Tags.concat(tags, "queue", delayedQueue),
            meterRegistry,
            delayedQueueDetail);
    verify(queueCounter, times(2)).registerQueue(any(), any(), any(), any(QueueDetail.class));
  }

  private RqueueMetrics rqueueMetrics(
      MeterRegistry meterRegistry, MetricsProperties metricsProperties)
      throws IllegalAccessException {
    RqueueMetrics metrics = new RqueueMetrics(template, queueCounter);
    FieldUtils.writeField(metrics, "meterRegistry", meterRegistry, true);
    FieldUtils.writeField(metrics, "metricsProperties", metricsProperties, true);
    return metrics;
  }

  @Test
  public void queueStatistics() throws IllegalAccessException {
    MeterRegistry meterRegistry = new SimpleMeterRegistry();
    RqueueMetrics metrics = rqueueMetrics(meterRegistry, metricsProperties);
    metrics.onApplicationEvent(new RqueueBootstrapEvent("Test", true));
    verifyQueueStatistics(meterRegistry, simpleQueue, 100, 10, 300, 0);
    verifyQueueStatistics(meterRegistry, delayedQueue, 200, 15, 0, 5);
  }

  @Test
  public void counterRegisterMethodIsCalled() throws IllegalAccessException {
    verifyCounterRegisterMethodIsCalled(Tags.empty());
  }

  @Test
  public void counterRegisterMethodIsCalledWithCorrectTag() throws IllegalAccessException {
    verifyCounterRegisterMethodIsCalled(tags);
  }
}
