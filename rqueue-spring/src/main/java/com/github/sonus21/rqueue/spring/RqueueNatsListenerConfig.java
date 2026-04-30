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
package com.github.sonus21.rqueue.spring;

import com.github.sonus21.rqueue.core.spi.MessageBroker;
import com.github.sonus21.rqueue.nats.JetStreamMessageBroker;
import io.nats.client.Connection;
import io.nats.client.JetStream;
import io.nats.client.JetStreamManagement;
import io.nats.client.Nats;
import io.nats.client.Options;
import java.io.IOException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.env.Environment;

/**
 * Non-Boot configuration that wires a JetStream-backed {@link MessageBroker} for Spring users
 * who opt in via {@code @EnableRqueue(backend = NATS)} or via the {@link NatsBackendCondition}
 * (jnats classpath + {@code rqueue.backend=nats}).
 */
@Configuration
public class RqueueNatsListenerConfig {

  @Autowired Environment environment;

  @Bean
  public Connection natsConnection() throws IOException {
    String url = environment.getProperty("rqueue.nats.url", Options.DEFAULT_URL);
    String username = environment.getProperty("rqueue.nats.username");
    String password = environment.getProperty("rqueue.nats.password");
    String token = environment.getProperty("rqueue.nats.token");
    String connectionName = environment.getProperty("rqueue.nats.connection-name");
    Options.Builder ob = new Options.Builder().server(url);
    if (connectionName != null) {
      ob.connectionName(connectionName);
    }
    if (token != null && !token.isEmpty()) {
      ob.token(token.toCharArray());
    } else if (username != null && password != null) {
      ob.userInfo(username, password);
    }
    try {
      return Nats.connect(ob.build());
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new IOException("Interrupted while connecting to NATS", e);
    }
  }

  @Bean
  public JetStream jetStream(Connection connection) throws IOException {
    return connection.jetStream();
  }

  @Bean
  public JetStreamManagement jetStreamManagement(Connection connection) throws IOException {
    return connection.jetStreamManagement();
  }

  @Bean
  public MessageBroker jetStreamMessageBroker(
      Connection connection, JetStream jetStream, JetStreamManagement jetStreamManagement) {
    return JetStreamMessageBroker.builder()
        .connection(connection)
        .jetStream(jetStream)
        .management(jetStreamManagement)
        .build();
  }
}
