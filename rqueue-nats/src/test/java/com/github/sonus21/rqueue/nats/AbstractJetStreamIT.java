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

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.github.sonus21.rqueue.enums.QueueType;
import com.github.sonus21.rqueue.listener.QueueDetail;
import io.nats.client.Connection;
import io.nats.client.Nats;
import io.nats.client.Options;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

/**
 * Base for JetStream integration tests. Mirrors the Redis test pattern: when {@code NATS_RUNNING}
 * is set the test connects to a locally running nats-server (CI path); otherwise a Testcontainers-
 * managed instance is started in {@link BeforeAll} (local Docker path). JUnit 5 / Testcontainers
 * skip the test gracefully if Docker isn't available and {@code NATS_RUNNING} isn't set.
 */
@Testcontainers(disabledWithoutDocker = true)
@NatsIntegrationTest
public abstract class AbstractJetStreamIT {

  static final boolean USE_EXTERNAL_NATS = System.getenv("NATS_RUNNING") != null;
  static final String EXTERNAL_NATS_URL =
      System.getenv().getOrDefault("NATS_URL", "nats://127.0.0.1:4222");

  /**
   * Container is only constructed in the local-Docker path. The Testcontainers extension
   * ignores static {@code GenericContainer} fields that aren't annotated {@code @Container};
   * we manage the container's lifecycle from {@link #setup()} / {@link #teardown()} so the
   * external-NATS path can leave it null without tripping the extension.
   */
  protected static GenericContainer<?> NATS;

  protected static Connection connection;

  @BeforeAll
  static void setup() throws Exception {
    String url;
    if (USE_EXTERNAL_NATS) {
      url = EXTERNAL_NATS_URL;
    } else {
      NATS = new GenericContainer<>(DockerImageName.parse("nats:2.10-alpine"))
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
    QueueDetail q = mock(QueueDetail.class);
    when(q.getName()).thenReturn(name);
    when(q.getType()).thenReturn(type);
    return q;
  }
}
