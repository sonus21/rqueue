/*
 * Copyright (c) 2024-2026 Sonu Kumar
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

import com.github.sonus21.rqueue.listener.QueueDetail;
import io.nats.client.Connection;
import io.nats.client.Nats;
import io.nats.client.Options;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

/**
 * Base for JetStream integration tests. Bound to Testcontainers; if Docker is unavailable JUnit 5
 * skips these tests automatically.
 */
@Testcontainers(disabledWithoutDocker = true)
abstract class AbstractJetStreamIT {

  @Container
  static final GenericContainer<?> NATS =
      new GenericContainer<>(DockerImageName.parse("nats:2.10-alpine"))
          .withCommand("-js", "-DV")
          .withExposedPorts(4222)
          .waitingFor(Wait.forLogMessage(".*Server is ready.*\\n", 1));

  protected static Connection connection;

  @BeforeAll
  static void connect() throws Exception {
    String url = "nats://" + NATS.getHost() + ":" + NATS.getMappedPort(4222);
    connection = Nats.connect(new Options.Builder().server(url).build());
  }

  @AfterAll
  static void disconnect() throws Exception {
    if (connection != null) {
      connection.close();
    }
  }

  protected QueueDetail mockQueue(String name) {
    QueueDetail q = mock(QueueDetail.class);
    when(q.getName()).thenReturn(name);
    return q;
  }
}
