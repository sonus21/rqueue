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
package com.github.sonus21.rqueue.spring.boot.integration;

import static org.assertj.core.api.Assertions.assertThat;

import com.github.sonus21.rqueue.annotation.RqueueListener;
import com.github.sonus21.rqueue.core.RqueueMessageEnqueuer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.data.redis.autoconfigure.DataRedisAutoConfiguration;
import org.springframework.boot.data.redis.autoconfigure.DataRedisReactiveAutoConfiguration;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.stereotype.Component;

/**
 * Verifies queue-level priority on the NATS backend: a single listener with
 * {@code priority="high=10,low=1"} consumes from two internal sub-queues
 * ({@code pq_high} and {@code pq_low}) and the producer sends to each via
 * {@link RqueueMessageEnqueuer#enqueueWithPriority}. We assert all 10 messages are
 * received and that 5 messages with payload prefix "high-" and 5 with "low-" arrive.
 */
@SpringBootTest(
    classes = NatsPriorityQueuesE2EIT.TestApp.class,
    properties = {"rqueue.backend=nats"})
@Tag("nats")
class NatsPriorityQueuesE2EIT extends AbstractNatsBootIT {

  @Autowired RqueueMessageEnqueuer enqueuer;

  @Autowired PriorityListener listener;

  @Test
  void messagesEnqueuedAtBothPrioritiesAreReceived() throws Exception {
    for (int i = 0; i < 5; i++) {
      enqueuer.enqueueWithPriority("pq", "high", "high-" + i);
      enqueuer.enqueueWithPriority("pq", "low", "low-" + i);
    }
    assertThat(listener.latch.await(30, TimeUnit.SECONDS)).isTrue();

    long highCount = listener.received.stream().filter(s -> s.startsWith("high-")).count();
    long lowCount = listener.received.stream().filter(s -> s.startsWith("low-")).count();
    assertThat(highCount).isEqualTo(5);
    assertThat(lowCount).isEqualTo(5);
  }

  @SpringBootApplication(
      exclude = {DataRedisAutoConfiguration.class, DataRedisReactiveAutoConfiguration.class})
  static class TestApp {}

  @Component
  static class PriorityListener {
    final CountDownLatch latch = new CountDownLatch(10);
    final List<String> received = Collections.synchronizedList(new ArrayList<>());

    @RqueueListener(value = "pq", priority = "high=10,low=1")
    void onMessage(String payload) {
      received.add(payload);
      latch.countDown();
    }
  }
}
