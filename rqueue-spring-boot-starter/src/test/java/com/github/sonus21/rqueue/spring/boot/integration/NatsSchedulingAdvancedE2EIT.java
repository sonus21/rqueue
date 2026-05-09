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
import com.github.sonus21.rqueue.core.Job;
import com.github.sonus21.rqueue.core.ReactiveRqueueMessageEnqueuer;
import com.github.sonus21.rqueue.core.RqueueMessageEnqueuer;
import com.github.sonus21.rqueue.core.spi.MessageBroker;
import com.github.sonus21.rqueue.listener.RqueueMessageHeaders;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Logger;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.data.redis.autoconfigure.DataRedisAutoConfiguration;
import org.springframework.boot.data.redis.autoconfigure.DataRedisReactiveAutoConfiguration;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Component;

/**
 * Advanced end-to-end tests for NATS message scheduling (ADR-51, NATS >= 2.12):
 *
 * <ol>
 *   <li>Recurring messages — a periodic message triggers the listener at least 3 times.
 *   <li>Retry on scheduled message — a delayed message fails twice then succeeds on the 3rd attempt.
 *   <li>Concurrent retry on delayed message — two pollers, one delayed message, fails once,
 *       retries, completes exactly once.
 *   <li>Concurrent retry on recurring message — two pollers, periodic message, no double
 *       processing per period.
 *   <li>Delayed message to DLQ — a scheduled message exhausts retries and lands on the DLQ.
 * </ol>
 *
 * <p>All tests skip automatically when the connected NATS server is older than 2.12 (detected via
 * {@link MessageBroker#capabilities()}).
 */
@SpringBootTest(
    classes = NatsSchedulingAdvancedE2EIT.TestApp.class,
    properties = {
        "rqueue.backend=nats",
        "rqueue.reactive.enabled=true",
        "rqueue.nats.stream-prefix=" + NatsSchedulingAdvancedE2EIT.STREAM_PREFIX,
        "rqueue.nats.subject-prefix=" + NatsSchedulingAdvancedE2EIT.SUBJECT_PREFIX
    })
@Tag("nats")
class NatsSchedulingAdvancedE2EIT extends AbstractNatsBootIT {

  static final String STREAM_PREFIX = "rqueue-js-schedAdv-";
  static final String SUBJECT_PREFIX = "rqueue.js.schedAdv.";

  @BeforeAll
  static void wipeOwnedStreams() {
    deleteStreamsWithPrefix(STREAM_PREFIX);
  }

  static final Duration DELAY = Duration.ofSeconds(3);
  static final Duration PERIOD = Duration.ofSeconds(5);
  static final Duration TOTAL_WAIT = Duration.ofSeconds(30);
  /** Quiesce window after a successful completion to rule out a racing duplicate. */
  static final Duration POST_SUCCESS_QUIESCE = Duration.ofSeconds(3);

  @Autowired
  RqueueMessageEnqueuer enqueuer;

  @Autowired
  ReactiveRqueueMessageEnqueuer reactiveEnqueuer;

  @Autowired
  MessageBroker broker;

  @Autowired
  RecurringListener recurringListener;

  @Autowired
  RetryOnScheduledListener retryListener;

  @Autowired
  ConcurrentRetryDelayedListener concurrentRetryListener;

  @Autowired
  ConcurrentRetryRecurringListener concurrentRecurringListener;

  @Autowired
  SchedDlqSourceListener schedDlqSourceListener;

  @Autowired
  SchedDlqSinkListener schedDlqSinkListener;

  @Autowired
  LongRunningJobListener longRunningJobListener;

  private void assumeScheduling() {
    Assumptions.assumeTrue(
        broker.capabilities().supportsDelayedEnqueue(),
        "Skipping: NATS server does not support message scheduling (< 2.12)");
  }

  // ---- 1. Recurring messages ------------------------------------------------

  @Test
  void recurringMessageIsInvokedMultipleTimes() throws Exception {
    assumeScheduling();
    enqueuer.enqueuePeriodic("adv-recur-e2e", "repeat-me", PERIOD);

    assertThat(recurringListener.latch.await(TOTAL_WAIT.toSeconds(), TimeUnit.SECONDS))
        .as("Expected at least 3 recurring invocations within %s", TOTAL_WAIT)
        .isTrue();
    assertThat(recurringListener.count.get())
        .as("Recurring message should have been delivered >= 3 times")
        .isGreaterThanOrEqualTo(3);
  }

  // ---- 2. Retry on scheduled message ----------------------------------------

  @Test
  void retryOnScheduledMessageEventuallySucceeds() throws Exception {
    assumeScheduling();
    enqueuer.enqueueIn("adv-retry-sched-e2e", "will-retry", DELAY);

    assertThat(retryListener.successLatch.await(TOTAL_WAIT.toSeconds(), TimeUnit.SECONDS))
        .as("Scheduled message should eventually succeed after 2 failed attempts")
        .isTrue();
    assertThat(retryListener.attempts.get())
        .as("Handler should have been invoked exactly 3 times (2 failures + 1 success)")
        .isEqualTo(3);
  }

  // ---- 3. Concurrent retry on delayed message --------------------------------

  @Test
  void concurrentRetryOnDelayedMessageCompletesExactlyOnce() throws Exception {
    assumeScheduling();
    enqueuer.enqueueIn("adv-conc-delayed-e2e", "conc-delay", DELAY);

    assertThat(concurrentRetryListener.successLatch.await(TOTAL_WAIT.toSeconds(), TimeUnit.SECONDS))
        .as("Concurrently retried delayed message should succeed")
        .isTrue();
    // Brief quiesce to rule out a racing second success from another poller
    Thread.sleep(POST_SUCCESS_QUIESCE.toMillis());
    assertThat(concurrentRetryListener.successCount.get())
        .as("Message must be processed exactly once despite concurrency=2 and a retry")
        .isEqualTo(1);
  }

  // ---- 4. Concurrent retry on recurring message ------------------------------

  @Test
  void concurrentRetryOnRecurringMessageNoDuplicates() throws Exception {
    assumeScheduling();
    enqueuer.enqueuePeriodic("adv-conc-recur-e2e", "conc-recur", PERIOD);

    assertThat(concurrentRecurringListener.latch.await(TOTAL_WAIT.toSeconds(), TimeUnit.SECONDS))
        .as("Recurring message with concurrency=2 should complete at least 2 periods")
        .isTrue();
    assertThat(concurrentRecurringListener.count.get())
        .as("Each period must run at least twice (2 periods delivered)")
        .isGreaterThanOrEqualTo(2);
  }

  // ---- 6. Long-running job: keep-alive via Job.updateVisibilityTimeout ------

  /**
   * A handler that runs for 3 minutes sends an "I'm alive" heartbeat every 30 seconds via
   * {@link Job#updateVisibilityTimeout}, which issues a NATS {@code +WIP} (work-in-progress)
   * signal and resets the consumer's {@code ackWait} timer. Without this the message would be
   * redelivered after each {@code ackWait} expiry; with it the job runs uninterrupted and
   * completes exactly once. The test latch opens after 3 "alive" signals (~90 s) so we don't
   * have to wait the full 3 minutes for the handler to exit.
   */
  @Test
  @Timeout(value = 5, unit = TimeUnit.MINUTES)
  void longRunningJobKeepsAliveViaVisibilityExtension() throws Exception {
    enqueuer.enqueue("long-running-e2e", "long-job");

    assertThat(longRunningJobListener.aliveLatch.await(3, TimeUnit.MINUTES))
        .as("Expected at least 3 keep-alive signals within 3 minutes")
        .isTrue();
    assertThat(longRunningJobListener.invocations.get())
        .as("Handler should have been invoked exactly once — no redelivery while WIP signals are"
            + " sent")
        .isEqualTo(1);
  }

  // ---- 5. Delayed message exhausts retries and goes to DLQ -------------------

  @Test
  void scheduledMessageExhaustsRetriesToDlq() throws Exception {
    assumeScheduling();
    enqueuer.enqueueIn("adv-sched-dlq-src", "dlq-payload", DELAY);

    assertThat(schedDlqSinkListener.latch.await(TOTAL_WAIT.toSeconds(), TimeUnit.SECONDS))
        .as("Dead-lettered scheduled message should appear in DLQ within %s", TOTAL_WAIT)
        .isTrue();
    assertThat(schedDlqSinkListener.received)
        .as("DLQ should contain the original payload")
        .containsExactly("dlq-payload");
    assertThat(schedDlqSourceListener.attempts.get())
        .as("Source handler should have been invoked exactly once (numRetries=1 → 0 retries)")
        .isEqualTo(1);
  }

  // ---- Spring application and listener components ---------------------------

  @SpringBootApplication(
      exclude = {DataRedisAutoConfiguration.class, DataRedisReactiveAutoConfiguration.class})
  @Import({
    RecurringListener.class,
    RetryOnScheduledListener.class,
    ConcurrentRetryDelayedListener.class,
    ConcurrentRetryRecurringListener.class,
    SchedDlqSourceListener.class,
    SchedDlqSinkListener.class,
    LongRunningJobListener.class
  })
  static class TestApp {}

  /** Listener for test 1: counts recurring invocations; latch opens at 3. */
  @Component
  static class RecurringListener {
    final AtomicInteger count = new AtomicInteger();
    final CountDownLatch latch = new CountDownLatch(3);

    @RqueueListener(value = "adv-recur-e2e")
    void onMessage(String payload) {
      count.incrementAndGet();
      latch.countDown();
    }
  }

  /**
   * Listener for test 2: fails first 2 invocations, succeeds on the 3rd.
   * {@code numRetries=3} gives maxDeliver=4 on the JetStream consumer, so rqueue retries up to 3
   * times (failureCount 0→1→2 < 3). The handler fails when attempts < 3.
   */
  @Component
  static class RetryOnScheduledListener {
    final AtomicInteger attempts = new AtomicInteger();
    final CountDownLatch successLatch = new CountDownLatch(1);

    @RqueueListener(value = "adv-retry-sched-e2e", numRetries = "3")
    void onMessage(String payload) {
      int attempt = attempts.incrementAndGet();
      if (attempt < 3) {
        throw new RuntimeException("simulated failure attempt=" + attempt);
      }
      successLatch.countDown();
    }
  }

  /**
   * Listener for test 3: concurrency=2, fails on the first delivery, succeeds on retry.
   * Verifies the message is completed exactly once despite two active pollers.
   */
  @Component
  static class ConcurrentRetryDelayedListener {
    final AtomicInteger attempts = new AtomicInteger();
    final AtomicInteger successCount = new AtomicInteger();
    final CountDownLatch successLatch = new CountDownLatch(1);

    @RqueueListener(value = "adv-conc-delayed-e2e", concurrency = "2", numRetries = "3")
    void onMessage(String payload) {
      int attempt = attempts.incrementAndGet();
      if (attempt == 1) {
        throw new RuntimeException("simulated failure on first delivery");
      }
      successCount.incrementAndGet();
      successLatch.countDown();
    }
  }

  /**
   * Listener for test 4: concurrency=2, periodic message; latch opens when 2 periods complete.
   * The dedup-key fix (id@processAt) ensures period 2+ is not silently dropped by JetStream.
   */
  @Component
  static class ConcurrentRetryRecurringListener {
    final AtomicInteger count = new AtomicInteger();
    final CountDownLatch latch = new CountDownLatch(2);

    @RqueueListener(value = "adv-conc-recur-e2e", concurrency = "2")
    void onMessage(String payload) {
      count.incrementAndGet();
      latch.countDown();
    }
  }

  /**
   * Source listener for test 5: always fails so the message exhausts its single retry and is
   * dead-lettered to {@code adv-sched-dlq}.
   */
  @Component
  static class SchedDlqSourceListener {
    final AtomicInteger attempts = new AtomicInteger();

    @RqueueListener(
        value = "adv-sched-dlq-src",
        deadLetterQueue = "adv-sched-dlq",
        numRetries = "1")
    void onMessage(String payload) {
      attempts.incrementAndGet();
      throw new RuntimeException("always-fail for DLQ test");
    }
  }

  /** DLQ sink for test 5: collects messages moved to the dead-letter queue. */
  @Component
  static class SchedDlqSinkListener {
    final CountDownLatch latch = new CountDownLatch(1);
    final List<String> received = Collections.synchronizedList(new ArrayList<>());

    @RqueueListener(value = "adv-sched-dlq")
    void onDlq(String payload) {
      received.add(payload);
      latch.countDown();
    }
  }

  /**
   * Listener for test 6: simulates a 3-minute job that sends a keep-alive heartbeat every 30 s.
   * {@code visibilityTimeout=60000} (60 s ackWait) is short enough that without keep-alive the
   * message would be redelivered before the job finishes. Calling
   * {@link Job#updateVisibilityTimeout} every 30 s issues a NATS {@code +WIP} signal, resetting
   * the ackWait timer and preventing redelivery. {@code aliveLatch} opens after 3 signals (~90 s).
   */
  @Component
  static class LongRunningJobListener {
    private static final Logger log = Logger.getLogger(LongRunningJobListener.class.getName());
    final AtomicInteger invocations = new AtomicInteger();
    final CountDownLatch aliveLatch = new CountDownLatch(3);

    @RqueueListener(value = "long-running-e2e", visibilityTimeout = "60000", numRetries = "3")
    void onMessage(String payload, @Header(RqueueMessageHeaders.JOB) Job job) {
      invocations.incrementAndGet();
      for (int i = 0; i < 6; i++) { // 6 × 30 s = 3 minutes
        try {
          Thread.sleep(Duration.ofSeconds(30).toMillis());
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          return;
        }
        boolean extended = job.updateVisibilityTimeout(Duration.ofSeconds(30));
        log.info("alive: jobId=" + job.getId()
            + " failureCount=" + job.getFailureCount()
            + " iteration=" + (i + 1)
            + " extended=" + extended);
        aliveLatch.countDown();
      }
    }
  }
}
