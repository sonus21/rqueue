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
package com.github.sonus21.rqueue.spring.boot.integration;

import static org.assertj.core.api.Assertions.assertThat;

import com.github.sonus21.rqueue.annotation.RqueueListener;
import com.github.sonus21.rqueue.core.ReactiveRqueueMessageEnqueuer;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.data.redis.autoconfigure.DataRedisAutoConfiguration;
import org.springframework.boot.data.redis.autoconfigure.DataRedisReactiveAutoConfiguration;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Flux;

/**
 * Verifies the reactive producer path on the NATS backend: enqueueing 5 messages via
 * {@link ReactiveRqueueMessageEnqueuer} (subscribed via {@link Flux#merge}) and confirming a
 * synchronous {@code @RqueueListener} on the same queue receives all 5.
 */
@SpringBootTest(
    classes = NatsReactiveEnqueueE2EIT.TestApp.class,
    properties = {"rqueue.backend=nats", "rqueue.reactive.enabled=true"})
@Tag("nats")
class NatsReactiveEnqueueE2EIT extends AbstractNatsBootIT {

  @Autowired
  ReactiveRqueueMessageEnqueuer reactiveEnqueuer;

  @Autowired
  ReactiveListener listener;

  @Test
  void reactivelyEnqueuedMessagesAreReceivedByListener() throws Exception {
    List<reactor.core.publisher.Mono<String>> publishers = new ArrayList<>();
    for (int i = 0; i < 5; i++) {
      publishers.add(reactiveEnqueuer.enqueue("reactive-e2e", "rx-" + i));
    }
    List<String> ids = Flux.merge(publishers).collectList().block(Duration.ofSeconds(15));
    assertThat(ids).hasSize(5);

    assertThat(listener.latch.await(20, TimeUnit.SECONDS)).isTrue();
    assertThat(listener.received).containsExactlyInAnyOrder("rx-0", "rx-1", "rx-2", "rx-3", "rx-4");
  }

  @SpringBootApplication(
      exclude = {DataRedisAutoConfiguration.class, DataRedisReactiveAutoConfiguration.class})
  static class TestApp {}

  @Component
  static class ReactiveListener {
    final CountDownLatch latch = new CountDownLatch(5);
    final List<String> received = Collections.synchronizedList(new ArrayList<>());

    @RqueueListener(value = "reactive-e2e")
    void onMessage(String payload) {
      received.add(payload);
      latch.countDown();
    }
  }
}
