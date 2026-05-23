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
package com.github.sonus21.rqueue.spring.boot.tests.integration;

import static org.assertj.core.api.Assertions.assertThat;

import com.github.sonus21.rqueue.annotation.RqueueListener;
import com.github.sonus21.rqueue.core.ReactiveRqueueMessageEnqueuer;
import com.github.sonus21.rqueue.core.RqueueMessageEnqueuer;
import com.github.sonus21.rqueue.spring.boot.tests.SpringBootIntegrationTest;
import com.github.sonus21.rqueue.test.application.BaseApplication;
import io.nats.client.Connection;
import io.nats.client.JetStreamApiException;
import io.nats.client.JetStreamManagement;
import io.nats.client.Nats;
import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.DockerClientFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerImageName;
import reactor.core.publisher.Flux;

@SpringBootTest(classes = BackendContractE2EIT.TestApp.class)
@SpringBootIntegrationTest
@Tag("nats")
class BackendContractE2EIT {

  private static final Logger log = Logger.getLogger(BackendContractE2EIT.class.getName());
  private static final String BACKEND =
      System.getProperty(
              "rqueue.test.backend", System.getenv().getOrDefault("RQUEUE_TEST_BACKEND", "redis"))
          .toLowerCase(Locale.ROOT);
  private static final String STREAM_PREFIX = "rqueue-js-backendContract-";
  private static final String SUBJECT_PREFIX = "rqueue.js.backendContract.";
  private static final String EXTERNAL_NATS_URL =
      System.getenv().getOrDefault("NATS_URL", "nats://127.0.0.1:4222");
  private static final boolean USE_EXTERNAL_NATS = System.getenv("NATS_RUNNING") != null;

  private static GenericContainer<?> nats;

  @Autowired
  RqueueMessageEnqueuer enqueuer;

  @Autowired
  ReactiveRqueueMessageEnqueuer reactiveEnqueuer;

  @Autowired
  ContractListener listener;

  @BeforeAll
  static void bootstrapBackend() {
    if (isNatsBackend()) {
      startNats();
      deleteStreamsWithPrefix(STREAM_PREFIX);
    }
  }

  @DynamicPropertySource
  static void properties(DynamicPropertyRegistry registry) {
    registry.add("rqueue.backend", () -> BACKEND);
    registry.add("rqueue.reactive.enabled", () -> "true");
    if (isNatsBackend()) {
      registry.add("rqueue.nats.connection.url", BackendContractE2EIT::activeNatsUrl);
      registry.add("rqueue.nats.naming.stream-prefix", () -> STREAM_PREFIX);
      registry.add("rqueue.nats.naming.subject-prefix", () -> SUBJECT_PREFIX);
    } else {
      registry.add("spring.data.redis.port", () -> "8028");
      registry.add("mysql.db.name", () -> "BackendContractE2EIT");
      registry.add("use.system.redis", () -> "false");
    }
  }

  @BeforeEach
  void resetListener() {
    listener.reset();
  }

  @Test
  void enqueuedMessagesAreReceivedByListener() throws Exception {
    for (int i = 0; i < 5; i++) {
      enqueuer.enqueue("contract-basic", "payload-" + i);
    }

    assertThat(listener.basicLatch.await(20, TimeUnit.SECONDS)).isTrue();
    assertThat(listener.basicReceived)
        .containsExactlyInAnyOrder("payload-0", "payload-1", "payload-2", "payload-3", "payload-4");
  }

  @Test
  void reactivelyEnqueuedMessagesAreReceivedByListener() throws Exception {
    List<reactor.core.publisher.Mono<String>> publishers = new ArrayList<>();
    for (int i = 0; i < 5; i++) {
      publishers.add(reactiveEnqueuer.enqueue("contract-reactive", "rx-" + i));
    }

    List<String> ids = Flux.merge(publishers).collectList().block(Duration.ofSeconds(15));
    assertThat(ids).hasSize(5).doesNotContainNull();
    assertThat(listener.reactiveLatch.await(20, TimeUnit.SECONDS)).isTrue();
    assertThat(listener.reactiveReceived)
        .containsExactlyInAnyOrder("rx-0", "rx-1", "rx-2", "rx-3", "rx-4");
  }

  @Test
  void concurrentListenerInvocationsAreObserved() throws Exception {
    for (int i = 0; i < 30; i++) {
      enqueuer.enqueue("contract-concurrency", "msg-" + i);
    }

    assertThat(listener.concurrencyLatch.await(45, TimeUnit.SECONDS)).isTrue();
    assertThat(listener.maxParallel.get()).isGreaterThanOrEqualTo(2);
  }

  @Test
  void messagesEnqueuedAtBothPrioritiesAreReceived() throws Exception {
    for (int i = 0; i < 5; i++) {
      enqueuer.enqueueWithPriority("contract-priority", "high", "high-" + i);
      enqueuer.enqueueWithPriority("contract-priority", "low", "low-" + i);
    }

    assertThat(listener.priorityLatch.await(30, TimeUnit.SECONDS)).isTrue();
    assertThat(listener.priorityReceived.stream().filter(s -> s.startsWith("high-")).count())
        .isEqualTo(5);
    assertThat(listener.priorityReceived.stream().filter(s -> s.startsWith("low-")).count())
        .isEqualTo(5);
  }

  private static boolean isNatsBackend() {
    return "nats".equalsIgnoreCase(BACKEND);
  }

  private static void startNats() {
    if (!isNatsBackend() || USE_EXTERNAL_NATS || nats != null) {
      return;
    }
    Assumptions.assumeTrue(
        DockerClientFactory.instance().isDockerAvailable(),
        "Skipping: Docker is not available and NATS_RUNNING is not set");
    nats = new GenericContainer<>(DockerImageName.parse("nats:2.12-alpine"))
        .withCommand("-js")
        .withExposedPorts(4222)
        .waitingFor(Wait.forLogMessage(".*Server is ready.*\\n", 1));
    nats.start();
    Runtime.getRuntime().addShutdownHook(new Thread(nats::stop));
  }

  private static String activeNatsUrl() {
    if (USE_EXTERNAL_NATS) {
      return EXTERNAL_NATS_URL;
    }
    startNats();
    return "nats://" + nats.getHost() + ":" + nats.getMappedPort(4222);
  }

  private static void deleteStreamsWithPrefix(String prefix) {
    try (Connection c = Nats.connect(activeNatsUrl())) {
      JetStreamManagement management = c.jetStreamManagement();
      List<String> names = management.getStreamNames();
      if (names == null) {
        return;
      }
      for (String name : names) {
        if (name.startsWith(prefix)) {
          management.deleteStream(name);
        }
      }
    } catch (IOException | JetStreamApiException e) {
      log.log(Level.WARNING, "Failed to clean NATS streams: " + e.getMessage());
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      log.log(Level.WARNING, "Failed to clean NATS streams: " + e.getMessage());
    }
  }

  @SpringBootApplication
  @Import({ContractListener.class, RedisTestConfig.class})
  static class TestApp {}

  @Configuration
  @ConditionalOnProperty(name = "rqueue.backend", havingValue = "redis", matchIfMissing = true)
  static class RedisTestConfig extends BaseApplication {}

  static class ContractListener {
    CountDownLatch basicLatch = new CountDownLatch(5);
    List<String> basicReceived = Collections.synchronizedList(new ArrayList<>());

    CountDownLatch reactiveLatch = new CountDownLatch(5);
    List<String> reactiveReceived = Collections.synchronizedList(new ArrayList<>());

    CountDownLatch concurrencyLatch = new CountDownLatch(30);
    AtomicInteger active = new AtomicInteger();
    AtomicInteger maxParallel = new AtomicInteger();

    CountDownLatch priorityLatch = new CountDownLatch(10);
    List<String> priorityReceived = Collections.synchronizedList(new ArrayList<>());

    void reset() {
      basicLatch = new CountDownLatch(5);
      basicReceived = Collections.synchronizedList(new ArrayList<>());
      reactiveLatch = new CountDownLatch(5);
      reactiveReceived = Collections.synchronizedList(new ArrayList<>());
      concurrencyLatch = new CountDownLatch(30);
      active = new AtomicInteger();
      maxParallel = new AtomicInteger();
      priorityLatch = new CountDownLatch(10);
      priorityReceived = Collections.synchronizedList(new ArrayList<>());
    }

    @RqueueListener(value = "contract-basic")
    void onBasic(String payload) {
      basicReceived.add(payload);
      basicLatch.countDown();
    }

    @RqueueListener(value = "contract-reactive")
    void onReactive(String payload) {
      reactiveReceived.add(payload);
      reactiveLatch.countDown();
    }

    @RqueueListener(value = "contract-concurrency", concurrency = "3")
    void onConcurrent(String payload) throws InterruptedException {
      int now = active.incrementAndGet();
      maxParallel.updateAndGet(curr -> Math.max(curr, now));
      try {
        Thread.sleep(200L);
      } finally {
        active.decrementAndGet();
        concurrencyLatch.countDown();
      }
    }

    @RqueueListener(
        value = "contract-priority",
        priority = "high=10,low=1",
        batchSize = "5",
        concurrency = "5")
    void onPriority(String payload) {
      priorityReceived.add(payload);
      priorityLatch.countDown();
    }
  }
}
