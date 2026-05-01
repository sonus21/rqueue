/*
 * Copyright (c) 2024-2026 Sonu Kumar
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * You may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 */
package com.github.sonus21.rqueue.nats.js;

import com.github.sonus21.rqueue.core.spi.MessageBroker;
import com.github.sonus21.rqueue.core.spi.MessageBrokerFactory;
import com.github.sonus21.rqueue.nats.RqueueNatsException;
import io.nats.client.Connection;
import io.nats.client.Nats;
import io.nats.client.Options;
import java.io.IOException;
import java.util.Map;
import java.util.Objects;

/**
 * ServiceLoader-discovered factory for the {@code "nats"} backend. Configuration keys (all
 * optional except {@code rqueue.nats.url}):
 *
 * <ul>
 *   <li>{@code rqueue.nats.url} - NATS URL, e.g. {@code nats://localhost:4222}</li>
 *   <li>{@code rqueue.nats.username} / {@code rqueue.nats.password} - basic auth</li>
 *   <li>{@code rqueue.nats.token} - token auth</li>
 *   <li>{@code rqueue.nats.connectionName} - friendly client name</li>
 * </ul>
 */
public class JetStreamMessageBrokerFactory implements MessageBrokerFactory {

  @Override
  public String name() {
    return "nats";
  }

  @Override
  public MessageBroker create(Map<String, String> config) {
    Objects.requireNonNull(config, "config must not be null");
    String url = config.getOrDefault("rqueue.nats.url", Options.DEFAULT_URL);
    String username = config.get("rqueue.nats.username");
    String password = config.get("rqueue.nats.password");
    String token = config.get("rqueue.nats.token");
    String connectionName = config.get("rqueue.nats.connectionName");

    Options.Builder ob = new Options.Builder().server(url);
    if (connectionName != null) {
      ob.connectionName(connectionName);
    }
    if (token != null && !token.isEmpty()) {
      ob.token(token.toCharArray());
    } else if (username != null && password != null) {
      ob.userInfo(username, password);
    }
    Connection connection;
    try {
      connection = Nats.connect(ob.build());
    } catch (IOException | InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new RqueueNatsException("Failed to connect to NATS at " + url, e);
    }
    return JetStreamMessageBroker.builder().connection(connection).build();
  }
}
