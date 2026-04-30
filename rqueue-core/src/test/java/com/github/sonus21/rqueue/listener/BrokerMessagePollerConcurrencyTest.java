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
package com.github.sonus21.rqueue.listener;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.github.sonus21.TestBase;
import com.github.sonus21.rqueue.CoreUnitTest;
import com.github.sonus21.rqueue.core.DefaultRqueueMessageConverter;
import com.github.sonus21.rqueue.core.RqueueMessage;
import com.github.sonus21.rqueue.core.spi.Capabilities;
import com.github.sonus21.rqueue.core.spi.MessageBroker;
import com.github.sonus21.rqueue.utils.backoff.FixedTaskExecutionBackOff;
import java.lang.reflect.Method;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import org.junit.jupiter.api.Test;
import org.springframework.messaging.converter.MessageConverter;
import org.springframework.messaging.handler.HandlerMethod;

/**
 * Verifies that running N {@link BrokerMessagePoller} instances against the same
 * {@code (queue, consumerName)} achieves parallel dispatch — the in-flight handler concurrency
 * matches the configured concurrency. This mirrors what the container does for
 * {@code @RqueueListener.concurrency > 1} on the NATS path.
 */
@CoreUnitTest
class BrokerMessagePollerConcurrencyTest extends TestBase {

  private final MessageConverter converter = new DefaultRqueueMessageConverter();

  static class GatedHandler {
    final AtomicInteger inFlight = new AtomicInteger();
    final AtomicInteger maxInFlight = new AtomicInteger();
    final AtomicInteger completed = new AtomicInteger();
    final CountDownLatch arrival;
    final CountDownLatch release;

    GatedHandler(int parties) {
      this.arrival = new CountDownLatch(parties);
      this.release = new CountDownLatch(1);
    }

    public void onMessage(String payload) throws InterruptedException {
      int now = inFlight.incrementAndGet();
      maxInFlight.accumulateAndGet(now, Math::max);
      arrival.countDown();
      // hold the worker so concurrent threads pile up
      release.await(2, TimeUnit.SECONDS);
      inFlight.decrementAndGet();
      completed.incrementAndGet();
    }
  }

  /** Shared fake broker; pop is thread-safe and serves messages one-at-a-time across pollers. */
  static class SharedFakeBroker implements MessageBroker {
    private final ConcurrentLinkedQueue<RqueueMessage> backlog;
    final ConcurrentLinkedQueue<RqueueMessage> ackd = new ConcurrentLinkedQueue<>();
    final ConcurrentLinkedQueue<RqueueMessage> nackd = new ConcurrentLinkedQueue<>();

    SharedFakeBroker(List<RqueueMessage> messages) {
      this.backlog = new ConcurrentLinkedQueue<>(messages);
    }

    @Override
    public void enqueue(QueueDetail q, RqueueMessage m) {}

    @Override
    public void enqueueWithDelay(QueueDetail q, RqueueMessage m, long delayMs) {}

    @Override
    public List<RqueueMessage> pop(QueueDetail q, String consumerName, int batch, Duration wait) {
      List<RqueueMessage> out = new ArrayList<>();
      // Hand out at most one message per pop so multiple pollers must run concurrently to drain.
      RqueueMessage m = backlog.poll();
      if (m != null) {
        out.add(m);
      } else {
        try {
          Thread.sleep(20);
        } catch (InterruptedException ie) {
          Thread.currentThread().interrupt();
        }
      }
      return out;
    }

    @Override
    public boolean ack(QueueDetail q, RqueueMessage m) {
      ackd.add(m);
      return true;
    }

    @Override
    public boolean nack(QueueDetail q, RqueueMessage m, long retryDelayMs) {
      nackd.add(m);
      return true;
    }

    @Override
    public long moveExpired(QueueDetail q, long now, int batch) {
      return 0;
    }

    @Override
    public List<RqueueMessage> peek(QueueDetail q, long offset, long count) {
      return Collections.emptyList();
    }

    @Override
    public long size(QueueDetail q) {
      return 0;
    }

    @Override
    public AutoCloseable subscribe(String channel, Consumer<String> handler) {
      return () -> {};
    }

    @Override
    public void publish(String channel, String payload) {}

    @Override
    public Capabilities capabilities() {
      return new Capabilities(true, false, false, false);
    }
  }

  private QueueDetail queueDetail() {
    return QueueDetail.builder()
        .name("q1")
        .queueName("__rq::queue::q1")
        .processingQueueName("__rq::pq::q1")
        .completedQueueName("__rq::cq::q1")
        .scheduledQueueName("__rq::sq::q1")
        .processingQueueChannelName("__rq::ch::q1")
        .scheduledQueueChannelName("__rq::sch::q1")
        .visibilityTimeout(30000)
        .numRetry(3)
        .batchSize(8)
        .active(true)
        .priority(Collections.emptyMap())
        .build();
  }

  private RqueueMessage message(String text) {
    org.springframework.messaging.Message<?> msg = converter.toMessage(text, null);
    String wire = (String) msg.getPayload();
    return RqueueMessage.builder()
        .id(UUID.randomUUID().toString())
        .queueName("q1")
        .message(wire)
        .build();
  }

  @Test
  void multiplePollersSharingConsumerDispatchInParallel() throws Exception {
    int concurrency = 3;
    int total = 6;
    GatedHandler bean = new GatedHandler(concurrency);
    List<RqueueMessage> messages = new ArrayList<>();
    for (int i = 0; i < total; i++) {
      messages.add(message("p-" + i));
    }
    SharedFakeBroker broker = new SharedFakeBroker(messages);

    Method method = GatedHandler.class.getMethod("onMessage", String.class);
    HandlerMethod handlerMethod = new HandlerMethod(bean, method);

    List<BrokerMessagePoller> pollers = new ArrayList<>();
    ExecutorService es = Executors.newFixedThreadPool(concurrency);
    for (int i = 0; i < concurrency; i++) {
      BrokerMessagePoller p = new BrokerMessagePoller(
          broker,
          queueDetail(),
          "consumer-A",
          handlerMethod,
          converter,
          new FixedTaskExecutionBackOff(50L, 3),
          null,
          4,
          Duration.ofMillis(20));
      pollers.add(p);
      es.submit(p);
    }

    // Wait for `concurrency` workers to be parked inside onMessage; if they did not run in
    // parallel this latch would never reach zero within the timeout.
    assertTrue(
        bean.arrival.await(2, TimeUnit.SECONDS),
        "expected " + concurrency + " concurrent in-flight handlers");
    assertEquals(concurrency, bean.maxInFlight.get(), "max in-flight should equal concurrency");

    // Release the gate so workers complete and continue draining the backlog.
    bean.release.countDown();

    long deadline = System.currentTimeMillis() + 3000;
    while (System.currentTimeMillis() < deadline && broker.ackd.size() < total) {
      Thread.sleep(20);
    }

    pollers.forEach(BrokerMessagePoller::stop);
    es.shutdown();
    assertTrue(es.awaitTermination(2, TimeUnit.SECONDS));

    assertEquals(total, broker.ackd.size(), "all messages should be acked exactly once");
    assertEquals(0, broker.nackd.size(), "no nacks expected on success");
    assertEquals(total, bean.completed.get(), "handler should run exactly once per message");
  }
}
