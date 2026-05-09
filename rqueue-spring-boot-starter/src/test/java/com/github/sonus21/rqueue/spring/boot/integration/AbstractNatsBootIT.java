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

import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeAll;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.DockerClientFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerImageName;

/**
 * Common Testcontainers + dynamic-property boilerplate for NATS-backed end-to-end tests.
 *
 * <p>Mirrors the existing Redis test pattern (see {@code RedisRunning} / {@code REDIS_RUNNING}):
 * when the {@code NATS_RUNNING} environment variable is set the tests assume an externally
 * managed nats-server is reachable at {@code NATS_URL} (default {@code nats://127.0.0.1:4222})
 * and skip Testcontainers entirely. CI sets {@code NATS_RUNNING=true} after starting nats-server
 * via apt; local dev leaves it unset and falls back to Testcontainers.
 *
 * <p>When neither {@code NATS_RUNNING} is set nor Docker is available the entire test class is
 * skipped via {@link org.junit.jupiter.api.Assumptions#assumeTrue} inside {@link #startNats()}.
 * This avoids the {@code @Testcontainers(disabledWithoutDocker=true)} pitfall where the annotation
 * silently disables tests even when {@code NATS_RUNNING=true} and Docker happens to be absent.
 *
 * <p>Subclasses declare their own {@code @SpringBootApplication} test config (typically excluding
 * Redis auto-config, see {@link NatsBackendEndToEndIT} for the reference pattern) and any
 * {@code @RqueueListener} beans they need.
 */
abstract class AbstractNatsBootIT {

  static final boolean USE_EXTERNAL_NATS = System.getenv("NATS_RUNNING") != null;

  static final String EXTERNAL_NATS_URL =
      System.getenv().getOrDefault("NATS_URL", "nats://127.0.0.1:4222");

  static GenericContainer<?> NATS;

  @BeforeAll
  static void startNats() {
    if (USE_EXTERNAL_NATS || NATS != null) {
      return;
    }
    Assumptions.assumeTrue(
        DockerClientFactory.instance().isDockerAvailable(),
        "Skipping: Docker is not available and NATS_RUNNING is not set — "
            + "start nats-server locally or set NATS_RUNNING=true");
    NATS = new GenericContainer<>(DockerImageName.parse("nats:2.12-alpine"))
        .withCommand("-js")
        .withExposedPorts(4222)
        .waitingFor(Wait.forLogMessage(".*Server is ready.*\\n", 1));
    NATS.start();
    Runtime.getRuntime().addShutdownHook(new Thread(NATS::stop));
  }

  @DynamicPropertySource
  static void natsProps(DynamicPropertyRegistry r) {
    if (USE_EXTERNAL_NATS) {
      r.add("rqueue.nats.connection.url", () -> EXTERNAL_NATS_URL);
    } else {
      r.add("rqueue.nats.connection.url", () -> {
        startNats();
        return "nats://" + NATS.getHost() + ":" + NATS.getMappedPort(4222);
      });
    }
  }
}
