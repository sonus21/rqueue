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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import com.github.sonus21.rqueue.core.RqueueMessageTemplate;
import com.github.sonus21.rqueue.listener.QueueDetail;
import com.github.sonus21.rqueue.utils.QueueUtils;
import com.github.sonus21.rqueue.event.QueueInitializationEvent;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tags;
import io.micrometer.core.instrument.search.MeterNotFoundException;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.util.HashMap;
import java.util.Map;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.StrictStubs.class)
public class RqueueMetricsTest {
  private RqueueMessageTemplate template = mock(RqueueMessageTemplate.class);
  private RqueueMetricsProperties metricsProperties = new RqueueMetricsProperties() {};
  private QueueCounter queueCounter = mock(QueueCounter.class);
  private Map<String, QueueDetail> queueDetails = new HashMap<>();
  private String simpleQueue = "simple-queue";
  private String delayedQueue = "delayed-queue";
  private String deadLetterQueue = "dlq";
  private Tags tags = Tags.of("rQueue", "dc1");

  @Before
  public void init() {
    queueDetails.put(simpleQueue, new QueueDetail(simpleQueue, -1, deadLetterQueue, false));
    queueDetails.put(delayedQueue, new QueueDetail(delayedQueue, -1, "", true));
    doAnswer(
            invocation -> {
              String zsetName = (String) invocation.getArguments()[0];
              if (zsetName.equals(QueueUtils.getTimeQueueName(delayedQueue))) {
                return 5L;
              }
              if (zsetName.equals(QueueUtils.getProcessingQueueName(simpleQueue))) {
                return 10L;
              }
              if (zsetName.equals(QueueUtils.getProcessingQueueName(delayedQueue))) {
                return 15L;
              }
              return null;
            })
        .when(template)
        .getZsetSize(anyString());

    doAnswer(
            invocation -> {
              String listName = (String) invocation.getArguments()[0];
              if (listName.equals(simpleQueue)) {
                return 100L;
              }
              if (listName.equals(delayedQueue)) {
                return 200L;
              }
              if (listName.equals(deadLetterQueue)) {
                return 300L;
              }
              return null;
            })
        .when(template)
        .getListLength(anyString());
  }

  private void verifyQueueDetail(
      MeterRegistry registry, String name, boolean deadLetter, boolean delayed, Tags expectedTags) {
    Tags tags = Tags.concat(expectedTags, "queue", name);
    registry.get("queue.size").tags(tags).gauge();
    registry.get("processing.queue.size").tags(tags).gauge();
    try {
      registry.get("dead.letter.queue.size").tags(tags).gauge();
      assertTrue(deadLetter);
    } catch (MeterNotFoundException e) {
      assertFalse(deadLetter);
    }
    try {
      registry.get("delayed.queue.size").tags(tags).gauge();
      assertTrue(delayed);
    } catch (MeterNotFoundException e) {
      assertFalse(delayed);
    }
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

  @Test
  public void queueStatistics() {
    MeterRegistry meterRegistry = new SimpleMeterRegistry();
    RqueueMetrics metrics =
        new RqueueMetrics(template, metricsProperties, meterRegistry, queueCounter);
    metrics.onApplicationEvent(new QueueInitializationEvent("Test", queueDetails, true));
    verifyQueueStatistics(meterRegistry, simpleQueue, 100, 10, 300, 0);
    verifyQueueStatistics(meterRegistry, delayedQueue, 200, 15, 0, 5);
  }

  private void verifyCounterRegisterMethodIsCalled(Tags tags) {
    MeterRegistry meterRegistry = new SimpleMeterRegistry();
    metricsProperties.setMetricTags(tags);
    RqueueMetrics metrics =
        new RqueueMetrics(template, metricsProperties, meterRegistry, queueCounter);
    metrics.onApplicationEvent(new QueueInitializationEvent("Test", queueDetails, true));
    verify(queueCounter, times(1))
        .registerQueue(
            metricsProperties, Tags.concat(tags, "queue", simpleQueue), meterRegistry, simpleQueue);
    verify(queueCounter, times(1))
        .registerQueue(
            metricsProperties,
            Tags.concat(tags, "queue", delayedQueue),
            meterRegistry,
            delayedQueue);
    verify(queueCounter, times(2)).registerQueue(any(), any(), any(), anyString());
  }

  @Test
  public void counterRegisterMethodIsCalled() {
    verifyCounterRegisterMethodIsCalled(Tags.empty());
  }

  @Test
  public void counterRegisterMethodIsCalledWithCorrectTag() {
    verifyCounterRegisterMethodIsCalled(tags);
  }
}
