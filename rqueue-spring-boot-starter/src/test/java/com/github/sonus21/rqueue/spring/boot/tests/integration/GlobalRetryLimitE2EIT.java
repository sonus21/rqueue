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
import com.github.sonus21.rqueue.config.RqueueConfig;
import com.github.sonus21.rqueue.config.SimpleRqueueListenerContainerFactory;
import com.github.sonus21.rqueue.core.RqueueMessageEnqueuer;
import com.github.sonus21.rqueue.spring.boot.tests.SpringBootIntegrationTest;
import com.github.sonus21.rqueue.test.application.BaseApplication;
import com.github.sonus21.rqueue.utils.backoff.FixedTaskExecutionBackOff;
import io.nats.client.Connection;
import io.nats.client.JetStreamApiException;
import io.nats.client.JetStreamManagement;
import io.nats.client.Nats;
import java.io.IOException;
import java.time.Duration;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.DockerClientFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerImageName;

@SpringBootTest(classes = GlobalRetryLimitE2EIT.TestApp.class)
@SpringBootIntegrationTest
@Tag("nats")
class GlobalRetryLimitE2EIT {

  private static final Logger log = Logger.getLogger(GlobalRetryLimitE2EIT.class.getName());
  private static final String BACKEND =
      System.getProperty(
              "rqueue.test.backend", System.getenv().getOrDefault("RQUEUE_TEST_BACKEND", "redis"))
          .toLowerCase(Locale.ROOT);
  private static final String QUEUE = "global-retry-" + BACKEND;
  private static final String STREAM_PREFIX = "rqueue-js-globalRetryE2E-";
  private static final String SUBJECT_PREFIX = "rqueue.js.globalRetryE2E.";
  private static final String EXTERNAL_NATS_URL =
      System.getenv().getOrDefault("NATS_URL", "nats://127.0.0.1:4222");
  private static final boolean USE_EXTERNAL_NATS = System.getenv("NATS_RUNNING") != null;

  private static GenericContainer<?> nats;

  @Autowired
  RqueueMessageEnqueuer enqueuer;

  @Autowired
  FailingListener listener;

  @Autowired
  RqueueConfig rqueueConfig;

  @Autowired(required = false)
  JetStreamManagement jsm;

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
    registry.add("rqueue.retry.max", () -> "2");
    registry.add("rqueue.retry.per.poll", () -> "1");
    registry.add("global.retry.limit.queue", () -> QUEUE);
    if (isNatsBackend()) {
      registry.add("rqueue.nats.connection.url", GlobalRetryLimitE2EIT::activeNatsUrl);
      registry.add("rqueue.nats.naming.stream-prefix", () -> STREAM_PREFIX);
      registry.add("rqueue.nats.naming.subject-prefix", () -> SUBJECT_PREFIX);
    } else {
      registry.add("spring.data.redis.port", () -> "8027");
      registry.add("mysql.db.name", () -> "GlobalRetryLimitE2EIT");
      registry.add("use.system.redis", () -> "false");
    }
  }

  @BeforeEach
  void resetListener() {
    listener.reset();
    rqueueConfig.setRetryPerPoll(1);
  }

  @Test
  void globalRetryLimitCapsSimpleEnqueueWhenRetryPerPollIsOne() throws Exception {
    enqueuer.enqueue(QUEUE, "payload");

    assertTwoAttemptsOnly();
    if (isNatsBackend()) {
      assertThat(jsm).isNotNull();
      assertThat(jsm.getConsumerInfo(STREAM_PREFIX + QUEUE, QUEUE + "-consumer")
              .getConsumerConfiguration()
              .getMaxDeliver())
          .isEqualTo(3L);
    }
  }

  @Test
  void globalRetryLimitCapsSimpleEnqueueWhenRetryPerPollIsHigh() throws Exception {
    rqueueConfig.setRetryPerPoll(100);

    enqueuer.enqueue(QUEUE, "payload");

    assertTwoAttemptsOnly();
  }

  @Test
  void globalRetryLimitUsesRemainingRetriesWhenRetryPerPollIncreases() throws Exception {
    enqueuer.enqueue(QUEUE, "payload");

    assertThat(listener.firstAttempt.await(20, TimeUnit.SECONDS)).isTrue();
    rqueueConfig.setRetryPerPoll(100);

    assertTwoAttemptsOnly();
  }

  private void assertTwoAttemptsOnly() throws InterruptedException {
    assertThat(listener.twoAttempts.await(20, TimeUnit.SECONDS)).isTrue();
    Awaitility.await()
        .during(Duration.ofMillis(600))
        .atMost(Duration.ofSeconds(3))
        .untilAsserted(() -> assertThat(listener.attempts).hasValue(2));
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
  @Import({FailingListener.class, RedisTestConfig.class})
  static class TestApp {

    @Bean
    public SimpleRqueueListenerContainerFactory simpleRqueueListenerContainerFactory() {
      FixedTaskExecutionBackOff backOff = new FixedTaskExecutionBackOff();
      backOff.setInterval(100);
      SimpleRqueueListenerContainerFactory factory = new SimpleRqueueListenerContainerFactory();
      factory.setTaskExecutionBackOff(backOff);
      return factory;
    }
  }

  @Configuration
  @ConditionalOnProperty(name = "rqueue.backend", havingValue = "redis", matchIfMissing = true)
  static class RedisTestConfig extends BaseApplication {}

  static class FailingListener {
    final AtomicInteger attempts = new AtomicInteger();
    CountDownLatch firstAttempt = new CountDownLatch(1);
    CountDownLatch twoAttempts = new CountDownLatch(2);

    void reset() {
      attempts.set(0);
      firstAttempt = new CountDownLatch(1);
      twoAttempts = new CountDownLatch(2);
    }

    @RqueueListener(value = "${global.retry.limit.queue}")
    void onMessage(String ignored) {
      attempts.incrementAndGet();
      firstAttempt.countDown();
      twoAttempts.countDown();
      throw new IllegalStateException("force retry");
    }
  }
}
