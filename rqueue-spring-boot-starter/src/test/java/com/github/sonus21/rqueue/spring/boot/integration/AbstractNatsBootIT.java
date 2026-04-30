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

import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

/**
 * Common Testcontainers + dynamic-property boilerplate for NATS-backed end-to-end tests.
 *
 * <p>Subclasses declare their own {@code @SpringBootApplication} test config (typically excluding
 * Redis auto-config, see {@link NatsBackendEndToEndIT} for the reference pattern) and any
 * {@code @RqueueListener} beans they need. The container is lifecycle-managed by the
 * {@link Testcontainers} extension and shared across all tests in a single subclass.
 */
@Testcontainers(disabledWithoutDocker = true)
abstract class AbstractNatsBootIT {

  @Container
  static final GenericContainer<?> NATS = new GenericContainer<>(
          DockerImageName.parse("nats:2.10-alpine"))
      .withCommand("-js")
      .withExposedPorts(4222)
      .waitingFor(Wait.forLogMessage(".*Server is ready.*\\n", 1));

  @DynamicPropertySource
  static void natsProps(DynamicPropertyRegistry r) {
    r.add(
        "rqueue.nats.connection.url",
        () -> "nats://" + NATS.getHost() + ":" + NATS.getMappedPort(4222));
  }
}
