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
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.data.redis.autoconfigure.DataRedisAutoConfiguration;
import org.springframework.boot.data.redis.autoconfigure.DataRedisReactiveAutoConfiguration;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.stereotype.Component;

/**
 * End-to-end test confirming that {@code @RqueueListener(concurrency=...)} actually runs more
 * than one handler invocation in parallel against the NATS backend. We don't assert an exact
 * parallelism value because JetStream prefetch + thread scheduling makes that flaky; observing
 * any parallelism &gt; 1 is enough proof the concurrency knob is wired through to a pull
 * subscription with multiple poller threads.
 */
@SpringBootTest(
    classes = NatsConcurrencyE2EIT.TestApp.class,
    properties = {"rqueue.backend=nats"})
@Tag("nats")
class NatsConcurrencyE2EIT extends AbstractNatsBootIT {

  @Autowired
  RqueueMessageEnqueuer enqueuer;

  @Autowired
  ConcurrencyListener listener;

  @Test
  void parallelInvocationsAreObserved() throws Exception {
    for (int i = 0; i < 30; i++) {
      enqueuer.enqueue("conc-e2e", "msg-" + i);
    }
    assertThat(listener.latch.await(45, TimeUnit.SECONDS)).isTrue();
    assertThat(listener.maxParallel.get())
        .as("at least 2 concurrent invocations should have been observed")
        .isGreaterThanOrEqualTo(2);
  }

  @SpringBootApplication(
      exclude = {DataRedisAutoConfiguration.class, DataRedisReactiveAutoConfiguration.class})
  @Import(ConcurrencyListener.class)
  static class TestApp {}

  @Component
  static class ConcurrencyListener {
    final CountDownLatch latch = new CountDownLatch(30);
    final AtomicInteger active = new AtomicInteger();
    final AtomicInteger maxParallel = new AtomicInteger();

    @RqueueListener(value = "conc-e2e", concurrency = "3")
    void onMessage(String payload) throws InterruptedException {
      int now = active.incrementAndGet();
      maxParallel.updateAndGet(curr -> Math.max(curr, now));
      try {
        Thread.sleep(200L);
      } finally {
        active.decrementAndGet();
        latch.countDown();
      }
    }
  }
}
