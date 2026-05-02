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
package com.github.sonus21.rqueue.spring;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

import com.github.sonus21.rqueue.core.spi.MessageBroker;
import com.github.sonus21.rqueue.nats.js.JetStreamMessageBroker;
import io.nats.client.Connection;
import io.nats.client.JetStream;
import io.nats.client.JetStreamManagement;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Tag("unit")
@Tag("nats")
class RqueueNatsListenerConfigTest {

  @Configuration
  static class MockBeans {
    @Bean
    Connection natsConnection() {
      return mock(Connection.class);
    }

    @Bean
    JetStream jetStream() {
      return mock(JetStream.class);
    }

    @Bean
    JetStreamManagement jetStreamManagement() {
      return mock(JetStreamManagement.class);
    }

    @Bean
    MessageBroker jetStreamMessageBroker(
        Connection connection, JetStream jetStream, JetStreamManagement management) {
      return JetStreamMessageBroker.builder()
          .connection(connection)
          .jetStream(jetStream)
          .management(management)
          .build();
    }
  }

  @Test
  void natsBrokerBeanIsRegistered() {
    AnnotationConfigApplicationContext ctx = new AnnotationConfigApplicationContext();
    ctx.register(MockBeans.class);
    ctx.refresh();
    MessageBroker broker = ctx.getBean(MessageBroker.class);
    assertNotNull(broker);
    assertTrue(broker instanceof JetStreamMessageBroker);
    ctx.close();
  }
}
