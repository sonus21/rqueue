/*
 * Copyright (c) 2026 Sonu Kumar
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * You may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 */
package com.github.sonus21.rqueue.spring.boot.integration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import com.github.sonus21.rqueue.annotation.RqueueListener;
import com.github.sonus21.rqueue.core.ReactiveRqueueMessageEnqueuer;
import com.github.sonus21.rqueue.core.RqueueMessageEnqueuer;
import com.github.sonus21.rqueue.nats.internal.NatsProvisioner;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.data.redis.autoconfigure.DataRedisAutoConfiguration;
import org.springframework.boot.data.redis.autoconfigure.DataRedisReactiveAutoConfiguration;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Flux;

/**
 * End-to-end test for NATS message scheduling (ADR-51, NATS >= 2.12). Verifies that messages
 * enqueued with a delay via {@link RqueueMessageEnqueuer#enqueueIn} and
 * {@link ReactiveRqueueMessageEnqueuer#enqueueIn} are held by JetStream until the scheduled
 * time before being delivered to a {@code @RqueueListener}.
 *
 * <p>Tests skip automatically when the connected NATS server is older than 2.12 (detected at
 * bootup by {@code NatsProvisioner}).
 */
@SpringBootTest(
    classes = NatsScheduledMessageE2EIT.TestApp.class,
    properties = {"rqueue.backend=nats", "rqueue.reactive.enabled=true"})
@Tag("nats")
class NatsScheduledMessageE2EIT extends AbstractNatsBootIT {

  static final Duration DELAY = Duration.ofSeconds(3);
  static final Duration NOT_YET_GUARD = Duration.ofMillis(800);
  static final Duration TOTAL_WAIT = Duration.ofSeconds(12);

  @Autowired
  NatsProvisioner natsProvisioner;

  @Autowired
  RqueueMessageEnqueuer enqueuer;

  @Autowired
  ReactiveRqueueMessageEnqueuer reactiveEnqueuer;

  @Autowired
  ScheduledListener listener;

  @BeforeEach
  void requireSchedulingSupport() {
    assumeTrue(
        natsProvisioner.isMessageSchedulingSupported(),
        "Skipping: connected NATS server is older than " + NatsProvisioner.SCHEDULING_MIN_VERSION
            + " and does not support ADR-51 message scheduling");
  }

  @Test
  void scheduledMessageIsDeliveredAfterDelay() throws Exception {
    enqueuer.enqueueIn("sched-e2e", "delayed-payload", DELAY);

    // Must not arrive before the delay window
    Thread.sleep(NOT_YET_GUARD.toMillis());
    assertThat(listener.syncReceived).doesNotContain("delayed-payload");

    // Must arrive after the delay (total latch wait generous enough for slow CI)
    assertThat(listener.syncLatch.await(TOTAL_WAIT.toSeconds(), TimeUnit.SECONDS))
        .as("Expected scheduled message to be delivered within %s", TOTAL_WAIT)
        .isTrue();
    assertThat(listener.syncReceived).containsExactly("delayed-payload");
  }

  @Test
  void reactiveScheduledMessageIsDeliveredAfterDelay() throws Exception {
    // Reset latch for the reactive queue
    List<reactor.core.publisher.Mono<String>> pubs = new ArrayList<>();
    pubs.add(reactiveEnqueuer.enqueueIn("rsched-e2e", "rx-delayed", DELAY));
    List<String> ids = Flux.merge(pubs).collectList().block(Duration.ofSeconds(10));
    assertThat(ids).hasSize(1).doesNotContainNull();

    Thread.sleep(NOT_YET_GUARD.toMillis());
    assertThat(listener.reactiveReceived).doesNotContain("rx-delayed");

    assertThat(listener.reactiveLatch.await(TOTAL_WAIT.toSeconds(), TimeUnit.SECONDS))
        .as("Expected reactive scheduled message to be delivered within %s", TOTAL_WAIT)
        .isTrue();
    assertThat(listener.reactiveReceived).containsExactly("rx-delayed");
  }

  @SpringBootApplication(
      exclude = {DataRedisAutoConfiguration.class, DataRedisReactiveAutoConfiguration.class})
  @Import(ScheduledListener.class)
  static class TestApp {}

  @Component
  static class ScheduledListener {
    final CountDownLatch syncLatch = new CountDownLatch(1);
    final List<String> syncReceived = Collections.synchronizedList(new ArrayList<>());

    final CountDownLatch reactiveLatch = new CountDownLatch(1);
    final List<String> reactiveReceived = Collections.synchronizedList(new ArrayList<>());

    @RqueueListener(value = "sched-e2e")
    void onSync(String payload) {
      syncReceived.add(payload);
      syncLatch.countDown();
    }

    @RqueueListener(value = "rsched-e2e")
    void onReactive(String payload) {
      reactiveReceived.add(payload);
      reactiveLatch.countDown();
    }
  }
}
