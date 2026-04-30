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
import org.junit.jupiter.api.Disabled;
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
 * <h2>Disabled: enqueue path is not yet routed through {@link com.github.sonus21.rqueue.core.spi.MessageBroker}</h2>
 *
 * Phases 1-4 + 3.5 wired the consumer side end-to-end ({@code BrokerMessagePoller} pops from
 * JetStream, deserializes via the configured converter, dispatches via reflection, and acks).
 * The producer side, however, still flows through {@code BaseMessageSender#enqueue} which calls
 * {@code RqueueMessageTemplate.addMessage(...)} (Redis RPUSH) and
 * {@code RqueueMessageMetadataService.save(...)} (Redis SET) unconditionally. There is no branch
 * yet that delegates to {@code MessageBroker.enqueue(QueueDetail, RqueueMessage)} when a non-Redis
 * broker is wired, and no escape from the Redis-backed metadata store. Until that delegation
 * lands, this test cannot pass without a Redis instance, defeating the whole point of running
 * Boot with {@code rqueue.backend=nats}.
 *
 * <p>Tracking item: route producer enqueue through {@code MessageBroker} when
 * {@code messageBroker.capabilities().usesPrimaryHandlerDispatch() == false}, and gate the
 * Redis-backed metadata store on the same flag.
 *
 * <p>This test is intentionally kept on disk (compiled, but {@link Disabled}) so the wiring fix
 * has a ready-made acceptance test and does not need to be reconstructed from scratch.
 */
@Disabled(
    "Blocked: producer enqueue path (BaseMessageSender#enqueue + RqueueMessageMetadataService) "
        + "is not yet routed through MessageBroker. With rqueue.backend=nats and no Redis "
        + "instance, the first enqueue throws because addMessage / metadata save still hit Redis. "
        + "Re-enable once enqueue delegation to MessageBroker.enqueue is wired.")
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
