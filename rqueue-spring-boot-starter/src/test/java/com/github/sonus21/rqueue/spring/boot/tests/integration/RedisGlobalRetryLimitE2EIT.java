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
package com.github.sonus21.rqueue.spring.boot.tests.integration;

import static org.assertj.core.api.Assertions.assertThat;

import com.github.sonus21.rqueue.annotation.RqueueListener;
import com.github.sonus21.rqueue.core.RqueueMessageEnqueuer;
import com.github.sonus21.rqueue.spring.boot.tests.SpringBootIntegrationTest;
import com.github.sonus21.rqueue.test.application.BaseApplication;
import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.test.context.TestPropertySource;

@SpringBootTest(classes = RedisGlobalRetryLimitE2EIT.TestApp.class)
@TestPropertySource(
    properties = {
      "rqueue.retry.max=2",
      "rqueue.retry.per.poll=1000",
      "spring.data.redis.port=8027",
      "mysql.db.name=RedisGlobalRetryLimitE2EIT",
      "use.system.redis=false"
    })
@SpringBootIntegrationTest
class RedisGlobalRetryLimitE2EIT {

  static final String QUEUE = "global-retry-redis";

  @Autowired
  RqueueMessageEnqueuer enqueuer;

  @Autowired
  FailingListener listener;

  @Test
  void globalRetryLimitCapsImplicitRetryForever() throws Exception {
    enqueuer.enqueue(QUEUE, "payload");

    assertThat(listener.twoAttempts.await(20, TimeUnit.SECONDS)).isTrue();
    Awaitility.await().during(Duration.ofSeconds(2)).atMost(Duration.ofSeconds(3)).untilAsserted(
        () -> assertThat(listener.attempts).hasValue(2));
  }

  @SpringBootApplication
  @Import(FailingListener.class)
  static class TestApp extends BaseApplication {}

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
