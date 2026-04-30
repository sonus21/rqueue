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
import java.net.ServerSocket;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.stereotype.Component;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;
import redis.embedded.RedisServer;

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
 * <h2>Why an embedded Redis is started</h2>
 *
 * Several rqueue beans (notably {@code RqueueConfig}, {@code RqueueQStatsDaoImpl}, and the
 * dashboard controller chain) currently still require a {@code RedisConnectionFactory} as a hard
 * Spring dependency, even when {@code rqueue.backend=nats}. Pure NATS-only deployments without
 * Redis on the classpath will need those beans to become conditional — tracked as a v1.x
 * follow-up. Until then this test starts an embedded Redis purely to satisfy the bean graph;
 * the actual message flow runs entirely through JetStream and never hits Redis.
 */
@SpringBootTest(
    classes = NatsBackendEndToEndIT.TestApp.class,
    properties = {"rqueue.backend=nats"})
@Testcontainers(disabledWithoutDocker = true)
@Tag("nats")
class NatsBackendEndToEndIT {

  private static RedisServer REDIS;
  private static int REDIS_PORT;

  @Container
  static final GenericContainer<?> NATS = new GenericContainer<>(
          DockerImageName.parse("nats:2.10-alpine"))
      .withCommand("-js")
      .withExposedPorts(4222)
      .waitingFor(Wait.forLogMessage(".*Server is ready.*\\n", 1));

  @BeforeAll
  static void startRedis() throws Exception {
    try (ServerSocket s = new ServerSocket(0)) {
      REDIS_PORT = s.getLocalPort();
    }
    REDIS = new RedisServer(REDIS_PORT);
    REDIS.start();
  }

  @AfterAll
  static void stopRedis() throws Exception {
    if (REDIS != null) {
      REDIS.stop();
    }
  }

  @DynamicPropertySource
  static void registerProps(DynamicPropertyRegistry r) {
    r.add(
        "rqueue.nats.connection.url",
        () -> "nats://" + NATS.getHost() + ":" + NATS.getMappedPort(4222));
    r.add("spring.data.redis.host", () -> "localhost");
    r.add("spring.data.redis.port", () -> REDIS_PORT);
  }

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

  @SpringBootApplication
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
