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
import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.awaitility.Awaitility;
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

@SpringBootTest(
    classes = NatsGlobalRetryLimitE2EIT.TestApp.class,
    properties = {
      "rqueue.backend=nats",
      "rqueue.retry.max=2",
      "rqueue.retry.per.poll=1000",
      "rqueue.nats.naming.stream-prefix=" + NatsGlobalRetryLimitE2EIT.STREAM_PREFIX,
      "rqueue.nats.naming.subject-prefix=" + NatsGlobalRetryLimitE2EIT.SUBJECT_PREFIX
    })
@Tag("nats")
class NatsGlobalRetryLimitE2EIT extends AbstractNatsBootIT {

  static final String QUEUE = "global-retry-nats";
  static final String STREAM_PREFIX = "rqueue-js-globalRetryE2E-";
  static final String SUBJECT_PREFIX = "rqueue.js.globalRetryE2E.";

  @BeforeAll
  static void wipeOwnedStreams() {
    deleteStreamsWithPrefix(STREAM_PREFIX);
  }

  @Autowired
  RqueueMessageEnqueuer enqueuer;

  @Autowired
  JetStreamManagement jsm;

  @Autowired
  FailingListener listener;

  @Test
  void globalRetryLimitCapsImplicitRetryForever() throws Exception {
    enqueuer.enqueue(QUEUE, "payload");

    assertThat(listener.twoAttempts.await(20, TimeUnit.SECONDS)).isTrue();
    Awaitility.await()
        .during(Duration.ofSeconds(2))
        .atMost(Duration.ofSeconds(3))
        .untilAsserted(() -> assertThat(listener.attempts).hasValue(2));

    assertThat(jsm.getConsumerInfo(STREAM_PREFIX + QUEUE, QUEUE + "-consumer")
            .getConsumerConfiguration()
            .getMaxDeliver())
        .isEqualTo(3L);
  }

  @SpringBootApplication(
      exclude = {DataRedisAutoConfiguration.class, DataRedisReactiveAutoConfiguration.class})
  @Import(FailingListener.class)
  static class TestApp {}

  @Component
  static class FailingListener {
    final AtomicInteger attempts = new AtomicInteger();
    final CountDownLatch twoAttempts = new CountDownLatch(2);

    @RqueueListener(value = QUEUE)
    void onMessage(String ignored) {
      attempts.incrementAndGet();
      twoAttempts.countDown();
      throw new IllegalStateException("force retry");
    }
  }
}
