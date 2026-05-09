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

import static org.assertj.core.api.Assertions.assertThat;

import com.github.sonus21.rqueue.annotation.RqueueListener;
import com.github.sonus21.rqueue.core.RqueueMessageEnqueuer;
import io.nats.client.JetStreamManagement;
import io.nats.client.api.ConsumerInfo;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.data.redis.autoconfigure.DataRedisAutoConfiguration;
import org.springframework.boot.data.redis.autoconfigure.DataRedisReactiveAutoConfiguration;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.stereotype.Component;

/**
 * Verifies that {@code @RqueueListener(consumerName="...")} causes the JetStream durable consumer
 * to be created with that exact name (rather than the auto-derived
 * {@code rqueue-<queue>-<bean>#<method>} form). After a message round-trips successfully, we
 * query the broker via {@link JetStreamManagement#getConsumerInfo} and assert the override is
 * present.
 */
@SpringBootTest(
    classes = NatsConsumerNameOverrideE2EIT.TestApp.class,
    properties = {
      "rqueue.backend=nats",
      "rqueue.nats.naming.stream-prefix=" + NatsConsumerNameOverrideE2EIT.STREAM_PREFIX,
      "rqueue.nats.naming.subject-prefix=" + NatsConsumerNameOverrideE2EIT.SUBJECT_PREFIX
    })
@Tag("nats")
class NatsConsumerNameOverrideE2EIT extends AbstractNatsBootIT {

  static final String STREAM_PREFIX = "rqueue-js-consumerOvrE2E-";
  static final String SUBJECT_PREFIX = "rqueue.js.consumerOvrE2E.";

  @BeforeAll
  static void wipeOwnedStreams() {
    deleteStreamsWithPrefix(STREAM_PREFIX);
  }

  @Autowired
  RqueueMessageEnqueuer enqueuer;

  @Autowired
  CustomConsumerListener listener;

  @Autowired
  JetStreamManagement jsm;

  @Test
  void overriddenConsumerNameIsRegisteredOnTheStream() throws Exception {
    enqueuer.enqueue("custom-consumer", "hello");
    assertThat(listener.latch.await(20, TimeUnit.SECONDS)).isTrue();

    ConsumerInfo info =
        jsm.getConsumerInfo(STREAM_PREFIX + "custom-consumer", "my-custom-consumer");
    assertThat(info).isNotNull();
    assertThat(info.getName()).isEqualTo("my-custom-consumer");
  }

  @SpringBootApplication(
      exclude = {DataRedisAutoConfiguration.class, DataRedisReactiveAutoConfiguration.class})
  @Import(CustomConsumerListener.class)
  static class TestApp {}

  @Component
  static class CustomConsumerListener {
    final CountDownLatch latch = new CountDownLatch(1);

    @RqueueListener(value = "custom-consumer", consumerName = "my-custom-consumer")
    void onMessage(String payload) {
      latch.countDown();
    }
  }
}
