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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.github.sonus21.TestBase;
import com.github.sonus21.rqueue.CoreUnitTest;
import com.github.sonus21.rqueue.config.MetricsProperties;
import com.github.sonus21.rqueue.core.EndpointRegistry;
import com.github.sonus21.rqueue.listener.QueueDetail;
import com.github.sonus21.rqueue.models.event.RqueueBootstrapEvent;
import com.github.sonus21.rqueue.utils.TestUtils;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tags;
import io.micrometer.core.instrument.search.MeterNotFoundException;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

@CoreUnitTest
class RqueueMetricsTest extends TestBase {

  private final MetricsProperties metricsProperties = new MetricsProperties() {};
  private final String simpleQueue = "simple-queue";
  private final String scheduledQueue = "scheduled-queue";
  private final String deadLetterQueue = "dlq";
  private final Tags tags = Tags.of("rQueue", "dc1");
  private final QueueDetail scheduledQueueDetail = TestUtils.createQueueDetail(scheduledQueue);
  private final QueueDetail simpleQueueDetail =
      TestUtils.createQueueDetail(simpleQueue, deadLetterQueue);

  @Mock
  private RqueueQueueMetricsProvider queueMetricsProvider;

  @Mock
  private QueueCounter queueCounter;

  @BeforeEach
  public void init() {
    MockitoAnnotations.openMocks(this);
    EndpointRegistry.delete();
    EndpointRegistry.register(simpleQueueDetail);
    EndpointRegistry.register(scheduledQueueDetail);
  }

  private void verifyQueueStatistics(
      MeterRegistry registry,
      String name,
      double queueSize,
      long processingQueueSize,
      long deadLetterQueueCount,
      long scheduledQueueSize) {
    Tags tags = Tags.of("queue", name);
    assertEquals(queueSize, registry.get("queue.size").tags(tags).gauge().value(), 0);
    assertEquals(
        processingQueueSize,
        registry.get("processing.queue.size").tags(tags).gauge().value(),
        0);
    try {
      double val = registry.get("dead.letter.queue.size").tags(tags).gauge().value();
      assertEquals(deadLetterQueueCount, val, 0);
    } catch (MeterNotFoundException e) {
      assertEquals(0, deadLetterQueueCount);
    }
    try {
      double val = registry.get("scheduled.queue.size").tags(tags).gauge().value();
      assertEquals(scheduledQueueSize, val, 0);
    } catch (MeterNotFoundException e) {
      assertEquals(0, scheduledQueueSize);
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
            Tags.concat(tags, "queue", scheduledQueue),
            meterRegistry,
            scheduledQueueDetail);
    verify(queueCounter, times(2)).registerQueue(any(), any(), any(), any(QueueDetail.class));
  }

  private RqueueMetrics rqueueMetrics(
      MeterRegistry meterRegistry, MetricsProperties metricsProperties)
      throws IllegalAccessException {
    RqueueMetrics metrics = new RqueueMetrics(queueCounter);
    FieldUtils.writeField(metrics, "meterRegistry", meterRegistry, true);
    FieldUtils.writeField(metrics, "metricsProperties", metricsProperties, true);
    FieldUtils.writeField(metrics, "queueMetricsProvider", queueMetricsProvider, true);
    return metrics;
  }

  @Test
  void queueStatistics() throws IllegalAccessException {
    // All four gauges read through RqueueQueueMetricsProvider, keyed by user-facing queue name.
    when(queueMetricsProvider.getPendingMessageCount(simpleQueue)).thenReturn(100L);
    when(queueMetricsProvider.getPendingMessageCount(scheduledQueue)).thenReturn(200L);
    when(queueMetricsProvider.getProcessingMessageCount(simpleQueue)).thenReturn(10L);
    when(queueMetricsProvider.getProcessingMessageCount(scheduledQueue)).thenReturn(15L);
    when(queueMetricsProvider.getScheduledMessageCount(simpleQueue)).thenReturn(0L);
    when(queueMetricsProvider.getScheduledMessageCount(scheduledQueue)).thenReturn(5L);
    // Only simpleQueue has a DLQ configured; the scheduledQueue gauge is never registered.
    when(queueMetricsProvider.getDeadLetterMessageCount(simpleQueue)).thenReturn(300L);

    MeterRegistry meterRegistry = new SimpleMeterRegistry();
    RqueueMetrics metrics = rqueueMetrics(meterRegistry, metricsProperties);
    metrics.onApplicationEvent(new RqueueBootstrapEvent("Test", true));
    verifyQueueStatistics(meterRegistry, simpleQueue, 100, 10, 300, 0);
    verifyQueueStatistics(meterRegistry, scheduledQueue, 200, 15, 0, 5);
  }

  @Test
  void counterRegisterMethodIsCalled() throws IllegalAccessException {
    verifyCounterRegisterMethodIsCalled(Tags.empty());
  }

  @Test
  void counterRegisterMethodIsCalledWithCorrectTag() throws IllegalAccessException {
    verifyCounterRegisterMethodIsCalled(tags);
  }
}
