/*
 * Copyright (c) 2026 Sonu Kumar
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * You may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 */
package com.github.sonus21.rqueue.nats;

import static org.junit.jupiter.api.Assumptions.assumeTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.github.sonus21.rqueue.enums.QueueType;
import com.github.sonus21.rqueue.listener.QueueDetail;
import io.nats.client.Connection;
import io.nats.client.Nats;
import io.nats.client.Options;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.testcontainers.DockerClientFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerImageName;

/**
 * Base for JetStream integration tests. Two execution paths:
 *
 * <ol>
 *   <li><b>External NATS</b> — set {@code NATS_RUNNING=1} (and optionally {@code NATS_URL}) to
 *       connect to an already-running nats-server. This is the path used in CI and when
 *       nats-server is installed locally (e.g. via Homebrew).
 *   <li><b>Testcontainers</b> — when Docker is available and {@code NATS_RUNNING} is not set, a
 *       {@code nats:2.12-alpine} container is started automatically.
 * </ol>
 *
 * <p>If neither path is viable the whole class is skipped via {@link
 * org.junit.jupiter.api.Assumptions#assumeTrue} — no hard failure.
 *
 * <p><b>Why no {@code @Testcontainers(disabledWithoutDocker = true)}:</b> that annotation
 * disables the class at the JUnit extension level <em>before</em> {@code @BeforeAll} runs,
 * so the external-NATS path would never be reached when Docker is absent. We manage the
 * container lifecycle manually and gate on assumptions instead.
 */
@NatsIntegrationTest
public abstract class AbstractJetStreamIT {

  static final boolean USE_EXTERNAL_NATS = System.getenv("NATS_RUNNING") != null;
  static final String EXTERNAL_NATS_URL =
      System.getenv().getOrDefault("NATS_URL", "nats://127.0.0.1:4222");

  protected static GenericContainer<?> NATS;
  protected static Connection connection;

  @BeforeAll
  static void setup() throws Exception {
    String url;
    if (USE_EXTERNAL_NATS) {
      url = EXTERNAL_NATS_URL;
    } else {
      assumeTrue(
          DockerClientFactory.instance().isDockerAvailable(),
          "Skipping NATS ITs: Docker is not available and NATS_RUNNING is not set. "
              + "Start nats-server locally and set NATS_RUNNING=1, or start Docker.");
      NATS = new GenericContainer<>(DockerImageName.parse("nats:2.12-alpine"))
          .withCommand("-js", "-DV")
          .withExposedPorts(4222)
          .waitingFor(Wait.forLogMessage(".*Server is ready.*\\n", 1));
      NATS.start();
      url = "nats://" + NATS.getHost() + ":" + NATS.getMappedPort(4222);
    }
    connection = Nats.connect(new Options.Builder().server(url).build());
  }

  @AfterAll
  static void teardown() throws Exception {
    if (connection != null) {
      connection.close();
    }
    if (NATS != null && NATS.isRunning()) {
      NATS.stop();
    }
  }

  protected QueueDetail mockQueue(String name) {
    return mockQueue(name, QueueType.QUEUE);
  }

  protected QueueDetail mockQueue(String name, QueueType type) {
    return mockQueue(name, type, null);
  }

  protected QueueDetail mockQueue(String name, QueueType type, String consumerName) {
    QueueDetail q = mock(QueueDetail.class);
    when(q.getName()).thenReturn(name);
    when(q.getType()).thenReturn(type);
    String resolved = consumerName != null ? consumerName : name + "-consumer";
    when(q.resolvedConsumerName()).thenReturn(resolved);
    return q;
  }
}
