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
package com.github.sonus21.rqueue.spring.boot;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

import com.github.sonus21.rqueue.core.spi.MessageBroker;
import com.github.sonus21.rqueue.nats.JetStreamMessageBroker;
import io.nats.client.Connection;
import io.nats.client.JetStream;
import io.nats.client.JetStreamManagement;
import org.junit.jupiter.api.Test;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

class RqueueNatsAutoConfigTest {

  private final ApplicationContextRunner runner = new ApplicationContextRunner()
      .withConfiguration(AutoConfigurations.of(RqueueNatsAutoConfig.class));

  @Test
  void doesNotWireBrokerWhenPropertyMissing() {
    runner.run(ctx -> assertThat(ctx).doesNotHaveBean(MessageBroker.class));
  }

  @Test
  void wiresJetStreamBrokerWhenPropertySetAndJnatsPresent() {
    runner
        .withPropertyValues("rqueue.backend=nats")
        .withUserConfiguration(MockNatsConfig.class)
        .run(ctx -> {
          assertThat(ctx).hasSingleBean(MessageBroker.class);
          assertThat(ctx.getBean(MessageBroker.class)).isInstanceOf(JetStreamMessageBroker.class);
        });
  }

  @Test
  void doesNotWireWhenBackendSetToOtherValue() {
    runner
        .withPropertyValues("rqueue.backend=redis")
        .withUserConfiguration(MockNatsConfig.class)
        .run(ctx -> assertThat(ctx).doesNotHaveBean(MessageBroker.class));
  }

  @Configuration
  static class MockNatsConfig {
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
  }
}
