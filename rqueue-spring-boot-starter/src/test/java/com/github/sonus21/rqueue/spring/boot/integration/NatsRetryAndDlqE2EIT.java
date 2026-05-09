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
import io.nats.client.api.StreamInfo;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicInteger;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.data.redis.autoconfigure.DataRedisAutoConfiguration;
import org.springframework.boot.data.redis.autoconfigure.DataRedisReactiveAutoConfiguration;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.stereotype.Component;

/**
 * After a handler exhausts {@code numRetries}, JetStream emits a max-deliveries advisory and the
 * broker's {@code installDeadLetterBridge} dispatcher republishes the payload onto the DLQ
 * stream. Currently disabled because {@link
 * com.github.sonus21.rqueue.spring.boot.RqueueNatsAutoConfig} does not yet invoke
 * {@code JetStreamMessageBroker.installDeadLetterBridge(...)} during container start, so dead-
 * lettered messages never reach the DLQ stream. Enable this test once that wiring is added.
 */
@SpringBootTest(
    classes = NatsRetryAndDlqE2EIT.TestApp.class,
    properties = {"rqueue.backend=nats"})
@Tag("nats")
@Disabled("This test exercises the NATS-native advisory bridge path"
    + " (JetStreamMessageBroker.installDeadLetterBridge /"
    + " $JS.EVENT.ADVISORY.CONSUMER.MAX_DELIVERIES), which is not yet wired by"
    + " RqueueNatsAutoConfig. The rqueue-level DLQ path (PostProcessingHandler → broker.moveToDlq)"
    + " is already tested in NatsSchedulingAdvancedE2EIT#scheduledMessageExhaustsRetriesToDlq and"
    + " works without this bridge. Enable this test once RqueueNatsAutoConfig provisions the"
    + " advisory dispatcher per queue during container start.")
class NatsRetryAndDlqE2EIT extends AbstractNatsBootIT {

  @Autowired
  RqueueMessageEnqueuer enqueuer;

  @Autowired
  FailingListener listener;

  @Autowired
  JetStreamManagement jsm;

  @Test
  void exhaustedMessageLandsOnDlqStream() {
    enqueuer.enqueue("failing", "boom");

    Awaitility.await().atMost(Duration.ofSeconds(60)).until(() -> listener.attempts.get() >= 2);

    Awaitility.await().atMost(Duration.ofSeconds(30)).untilAsserted(() -> {
      StreamInfo dlq = jsm.getStreamInfo("rqueue-js-failing-dlq");
      assertThat(dlq.getStreamState().getMsgCount()).isGreaterThanOrEqualTo(1);
    });
  }

  @SpringBootApplication(
      exclude = {DataRedisAutoConfiguration.class, DataRedisReactiveAutoConfiguration.class})
  static class TestApp {}

  @Component
  static class FailingListener {
    final AtomicInteger attempts = new AtomicInteger();

    @RqueueListener(value = "failing", numRetries = "2")
    void onMessage(String payload) {
      attempts.incrementAndGet();
      throw new RuntimeException("simulated failure for payload=" + payload);
    }
  }
}
