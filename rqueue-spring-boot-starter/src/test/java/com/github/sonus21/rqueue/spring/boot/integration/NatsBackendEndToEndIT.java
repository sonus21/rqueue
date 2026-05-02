/*
 * Copyright (c) 2026 Sonu Kumar
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
import org.springframework.context.annotation.Import;
import org.springframework.stereotype.Component;

/**
 * End-to-end integration test wiring a Spring Boot application against a NATS JetStream
 * instance via {@code rqueue.backend=nats}, an {@link RqueueListener}, and the default
 * {@link RqueueMessageEnqueuer}. It exercises the full intended path:
 *
 * <pre>
 *   Enqueue -> JetStreamMessageBroker.enqueue -> JetStream stream
 *           -> BrokerMessagePoller.pop -> @RqueueListener invocation -> broker.ack
 * </pre>
 *
 * <p>The NATS instance is supplied by {@link AbstractNatsBootIT}: when {@code NATS_RUNNING=true}
 * (CI), the test connects to a locally running nats-server; otherwise it falls back to a
 * Testcontainers-managed container, which itself skips gracefully without Docker.
 *
 * <p>Boots without any Redis at all: every Redis-shaped bean (config DAOs, dashboard controllers,
 * pub/sub channel, schedulers) is gated by {@code @Conditional(RedisBackendCondition.class)} and
 * stays out of the context when {@code rqueue.backend=nats}. {@code DataRedisAutoConfiguration}
 * is excluded so Spring Boot doesn't try to wire a Lettuce client either.
 */
@SpringBootTest(
    classes = NatsBackendEndToEndIT.TestApp.class,
    properties = {"rqueue.backend=nats"})
@Tag("nats")
class NatsBackendEndToEndIT extends AbstractNatsBootIT {

  @Autowired
  RqueueMessageEnqueuer enqueuer;

  @Autowired
  TestListener listener;

  @Test
  void enqueueIsReceivedByListener() throws Exception {
    for (int i = 0; i < 5; i++) {
      enqueuer.enqueue("e2e-test", "payload-" + i);
    }
    assertThat(listener.latch.await(20, TimeUnit.SECONDS)).isTrue();
    assertThat(listener.received)
        .containsExactlyInAnyOrder("payload-0", "payload-1", "payload-2", "payload-3", "payload-4");
  }

  @SpringBootApplication(
      exclude = {DataRedisAutoConfiguration.class, DataRedisReactiveAutoConfiguration.class})
  @Import(TestListener.class)
  static class TestApp {}

  @Component
  static class TestListener {
    final CountDownLatch latch = new CountDownLatch(5);
    final List<String> received = Collections.synchronizedList(new ArrayList<>());

    @RqueueListener(value = "e2e-test")
    void onMessage(String payload) {
      received.add(payload);
      latch.countDown();
    }
  }
}
