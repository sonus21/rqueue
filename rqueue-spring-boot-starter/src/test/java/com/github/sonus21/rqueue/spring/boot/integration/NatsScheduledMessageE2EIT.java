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
import org.junit.jupiter.api.BeforeAll;
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
    properties = {
        "rqueue.backend=nats",
        "rqueue.reactive.enabled=true",
        // Per-class prefix isolates this test's NATS streams from every other NATS-backed test
        // running against the same nats-server (CI shares one instance across all classes, and a
        // persistent JetStream dir survives reruns). Same queue name → distinct stream so we never
        // inherit stale config or in-flight messages from an earlier class/run.
        "rqueue.nats.stream-prefix=" + NatsScheduledMessageE2EIT.STREAM_PREFIX,
        "rqueue.nats.subject-prefix=" + NatsScheduledMessageE2EIT.SUBJECT_PREFIX
    })
@Tag("nats")
class NatsScheduledMessageE2EIT extends AbstractNatsBootIT {

  static final String STREAM_PREFIX = "rqueue-js-schedE2E-";
  static final String SUBJECT_PREFIX = "rqueue.js.schedE2E.";

  @BeforeAll
  static void wipeOwnedStreams() {
    deleteStreamsWithPrefix(STREAM_PREFIX);
  }

  static final Duration DELAY = Duration.ofSeconds(3);
  static final Duration NOT_YET_GUARD = Duration.ofMillis(800);
  static final Duration TOTAL_WAIT = Duration.ofSeconds(12);

  /**
   * Longer delay for the "enqueue-first, then enqueueIn" regression test.  The stream upgrade
   * (updateStream round-trip + consumer re-creation) adds latency before the scheduler header is
   * accepted, so we use 10 s to give the system enough headroom that the "not-yet" assertion
   * cannot race with the scheduler firing.
   */
  static final Duration ETD_DELAY = Duration.ofSeconds(10);
  static final Duration ETD_TOTAL_WAIT = Duration.ofSeconds(20);

  @Autowired
  NatsProvisioner natsProvisioner;

  @Autowired
  RqueueMessageEnqueuer enqueuer;

  @Autowired
  ReactiveRqueueMessageEnqueuer reactiveEnqueuer;

  @Autowired
  ScheduledListener listener;

  @Autowired
  EtdListener etdListener;

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

  /**
   * Regression test for the "enqueue-first, then enqueueIn" stream-upgrade path.
   *
   * <p>When a plain {@link RqueueMessageEnqueuer#enqueue enqueue()} call happens before the first
   * {@link RqueueMessageEnqueuer#enqueueIn enqueueIn()} call the stream is provisioned without
   * the {@code allow_msg_schedules} flag and without the sched-wildcard subject. The provisioner
   * must detect this on the delayed call and upgrade the stream in-place (add both the flag and the
   * sched-wildcard subject via a single {@code updateStream()}) before publishing the scheduled
   * message — otherwise NATS rejects the publish with "no stream matches subject".
   *
   * <p>Expected behaviour:
   * <ol>
   *   <li>The immediate message is delivered right away (stream has it before any upgrade).</li>
   *   <li>After the stream upgrade the scheduled message is held by JetStream until
   *       {@code ETD_DELAY} has passed.</li>
   *   <li>Both messages arrive within {@code ETD_TOTAL_WAIT}.</li>
   * </ol>
   */
  @Test
  void enqueueFirst_thenEnqueueIn_streamUpgradedAndBothDelivered() throws Exception {
    // Step 1: plain enqueue — stream is created with only the work subject, no sched flag
    enqueuer.enqueue("etd-e2e", "immediate");

    // Step 2: delayed enqueue — provisioner must upgrade the stream in-place
    enqueuer.enqueueIn("etd-e2e", "delayed", ETD_DELAY);

    // Immediate message must arrive before the ETD_DELAY fires
    assertThat(etdListener.immediateLatch.await(TOTAL_WAIT.toSeconds(), TimeUnit.SECONDS))
        .as("Immediate message must be delivered before delay fires")
        .isTrue();
    assertThat(etdListener.received)
        .as("Only the immediate message must have arrived at this point")
        .containsExactly("immediate");

    // Delayed message must NOT be visible yet (delay is 10 s, assertion runs < 1 s after immediate)
    assertThat(etdListener.received).doesNotContain("delayed");

    // Both messages must eventually arrive within the generous total-wait window
    assertThat(etdListener.allLatch.await(ETD_TOTAL_WAIT.toSeconds(), TimeUnit.SECONDS))
        .as("Both messages must be delivered within %s", ETD_TOTAL_WAIT)
        .isTrue();
    assertThat(etdListener.received)
        .as("Both immediate and delayed messages must have been received")
        .containsExactlyInAnyOrder("immediate", "delayed");
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
  @Import({ScheduledListener.class, EtdListener.class})
  static class TestApp {}

  /**
   * Listener for the "enqueue-first, then enqueueIn" regression test queue.
   *
   * <p>{@code immediateLatch} counts down on the very first message received (used to assert the
   * immediate message arrived before the delay fires). {@code allLatch} counts down twice — once
   * per message — and reaching zero signals that both the immediate and delayed messages were
   * consumed.
   */
  @Component
  static class EtdListener {
    final CountDownLatch immediateLatch = new CountDownLatch(1);
    final CountDownLatch allLatch = new CountDownLatch(2);
    final List<String> received = Collections.synchronizedList(new ArrayList<>());

    @RqueueListener(value = "etd-e2e")
    void onMessage(String payload) {
      received.add(payload);
      immediateLatch.countDown(); // first call unblocks; subsequent calls are no-ops
      allLatch.countDown();       // each call decrements; reaches 0 when both arrive
    }
  }

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
