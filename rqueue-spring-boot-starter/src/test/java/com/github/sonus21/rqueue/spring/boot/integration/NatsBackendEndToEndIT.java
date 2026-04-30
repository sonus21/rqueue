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
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.data.redis.autoconfigure.DataRedisAutoConfiguration;
import org.springframework.boot.data.redis.autoconfigure.DataRedisReactiveAutoConfiguration;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.stereotype.Component;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

/**
 * End-to-end integration test wiring a Spring Boot application against a Testcontainers-managed
 * NATS JetStream instance via {@code rqueue.backend=nats}, an {@link RqueueListener}, and the
 * default {@link RqueueMessageEnqueuer}. It exercises the full intended path:
 *
 * <pre>
 *   Enqueue -> JetStreamMessageBroker.enqueue -> JetStream stream
 *           -> BrokerMessagePoller.pop -> @RqueueListener invocation -> broker.ack
 * </pre>
 *
 * <h2>Why Redis auto-config is excluded</h2>
 *
 * The starter declares {@code spring-boot-starter-data-redis} as an {@code api} dependency, so
 * Spring Boot would try to auto-configure a {@code LettuceConnectionFactory} at startup and fail
 * the context because no Redis instance is available in this test. Excluding the Redis
 * auto-config classes on {@link TestApp} (rather than via property) keeps the exclusion local to
 * this test and visible to readers.
 *
 * <h2>Producer path</h2>
 *
 * The sync producer flows through {@code BaseMessageSender#enqueue} which now delegates to
 * {@code MessageBroker.enqueue(QueueDetail, RqueueMessage)} when the active broker advertises
 * {@code !usesPrimaryHandlerDispatch()}, and {@code storeMessageMetadata} short-circuits on the
 * same flag so no Redis HASH write is attempted. Together with the broker-driven poller this
 * exercises the full produce-and-consume loop without touching Redis.
 */
@SpringBootTest(
    classes = NatsBackendEndToEndIT.TestApp.class,
    properties = {"rqueue.backend=nats"})
@Testcontainers(disabledWithoutDocker = true)
class NatsBackendEndToEndIT {

  @Container
  static final GenericContainer<?> NATS =
      new GenericContainer<>(DockerImageName.parse("nats:2.10-alpine"))
          .withCommand("-js")
          .withExposedPorts(4222)
          .waitingFor(Wait.forLogMessage(".*Server is ready.*\\n", 1));

  @DynamicPropertySource
  static void natsProps(DynamicPropertyRegistry r) {
    r.add(
        "rqueue.nats.connection.url",
        () -> "nats://" + NATS.getHost() + ":" + NATS.getMappedPort(4222));
  }

  @Autowired RqueueMessageEnqueuer enqueuer;
  @Autowired TestListener listener;

  @Test
  void enqueueIsReceivedByListener() throws Exception {
    for (int i = 0; i < 5; i++) {
      enqueuer.enqueue("e2e-test", "payload-" + i);
    }
    assertThat(listener.latch.await(20, TimeUnit.SECONDS)).isTrue();
    assertThat(listener.received)
        .containsExactlyInAnyOrder(
            "payload-0", "payload-1", "payload-2", "payload-3", "payload-4");
  }

  @SpringBootApplication(
      exclude = {DataRedisAutoConfiguration.class, DataRedisReactiveAutoConfiguration.class})
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
