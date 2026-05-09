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
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.BeforeAll;
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
 * Two {@code @RqueueListener} methods on the same queue should each receive every message
 * published to it (independent durable consumers, fan-out semantics). Currently disabled because
 * the default JetStream stream retention is {@code WorkQueue}, which permits only one
 * filter-overlapping consumer at a time and deletes a message after the first ack — so true
 * fan-out is not possible with the v1 broker defaults. To enable this test, the broker would
 * need to switch to {@code Limits} or {@code Interest} retention for queues with multiple
 * listeners.
 */
@SpringBootTest(
    classes = NatsMultipleListenersOnSameQueueE2EIT.TestApp.class,
    properties = {
        "rqueue.backend=nats",
        "rqueue.nats.naming.stream-prefix=" + NatsMultipleListenersOnSameQueueE2EIT.STREAM_PREFIX,
        "rqueue.nats.naming.subject-prefix=" + NatsMultipleListenersOnSameQueueE2EIT.SUBJECT_PREFIX
    })
@Tag("nats")
@Disabled("Default JetStream retention=WorkQueue prevents true fan-out across multiple consumers; "
    + "enable once retention is configurable per queue or defaulted to Limits/Interest.")
class NatsMultipleListenersOnSameQueueE2EIT extends AbstractNatsBootIT {

  static final String STREAM_PREFIX = "rqueue-js-multiListenerE2E-";
  static final String SUBJECT_PREFIX = "rqueue.js.multiListenerE2E.";

  @BeforeAll
  static void wipeOwnedStreams() {
    deleteStreamsWithPrefix(STREAM_PREFIX);
  }

  @Autowired
  RqueueMessageEnqueuer enqueuer;

  @Autowired
  ListenerOne one;

  @Autowired
  ListenerTwo two;

  @Test
  void bothListenersReceiveAllMessages() throws Exception {
    for (int i = 0; i < 5; i++) {
      enqueuer.enqueue("multi", "fan-" + i);
    }
    assertThat(one.latch.await(20, TimeUnit.SECONDS)).isTrue();
    assertThat(two.latch.await(20, TimeUnit.SECONDS)).isTrue();
    assertThat(one.received).hasSize(5);
    assertThat(two.received).hasSize(5);
  }

  @SpringBootApplication(
      exclude = {DataRedisAutoConfiguration.class, DataRedisReactiveAutoConfiguration.class})
  static class TestApp {}

  @Component
  static class ListenerOne {
    final CountDownLatch latch = new CountDownLatch(5);
    final List<String> received = Collections.synchronizedList(new ArrayList<>());

    @RqueueListener(value = "multi")
    void onMessage(String payload) {
      received.add(payload);
      latch.countDown();
    }
  }

  @Component
  static class ListenerTwo {
    final CountDownLatch latch = new CountDownLatch(5);
    final List<String> received = Collections.synchronizedList(new ArrayList<>());

    @RqueueListener(value = "multi")
    void onMessage(String payload) {
      received.add(payload);
      latch.countDown();
    }
  }
}
