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
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.github.sonus21.TestBase;
import com.github.sonus21.rqueue.CoreUnitTest;
import com.github.sonus21.rqueue.core.DefaultRqueueMessageConverter;
import com.github.sonus21.rqueue.core.RqueueMessage;
import com.github.sonus21.rqueue.core.spi.Capabilities;
import com.github.sonus21.rqueue.core.spi.MessageBroker;
import com.github.sonus21.rqueue.listener.RqueueMessageListenerContainer.QueueStateMgr;
import com.github.sonus21.rqueue.utils.backoff.FixedTaskExecutionBackOff;
import java.lang.reflect.Method;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
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

@CoreUnitTest
class BrokerMessagePollerTest extends TestBase {

  private final MessageConverter converter = new DefaultRqueueMessageConverter();

  static class StringHandler {
    final List<String> received = Collections.synchronizedList(new ArrayList<>());
    volatile boolean throwOnInvoke = false;

    public void onMessage(String payload) {
      if (throwOnInvoke) {
        throw new RuntimeException("boom");
      }
      received.add(payload);
    }
  }

  private static QueueDetail queueDetail() {
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

  private RqueueMessage rqMessage(String text) {
    org.springframework.messaging.Message<?> msg = converter.toMessage(text, null);
    String wire = (String) msg.getPayload();
    return RqueueMessage.builder()
        .id(UUID.randomUUID().toString())
        .queueName("q1")
        .message(wire)
        .build();
  }

  private HandlerMethod handlerMethodFor(StringHandler bean) throws Exception {
    Method m = StringHandler.class.getMethod("onMessage", String.class);
    return new HandlerMethod(bean, m);
  }

  /** Simple in-memory broker double for unit tests. */
  static class FakeBroker implements MessageBroker {
    final ConcurrentLinkedQueue<RqueueMessage> ackd = new ConcurrentLinkedQueue<>();
    final ConcurrentLinkedQueue<RqueueMessage> nackd = new ConcurrentLinkedQueue<>();
    final ConcurrentLinkedQueue<Long> nackDelays = new ConcurrentLinkedQueue<>();
    private final List<List<RqueueMessage>> popResponses;
    private final AtomicInteger popCalls = new AtomicInteger();

    FakeBroker(List<List<RqueueMessage>> popResponses) {
      this.popResponses = popResponses;
    }

    @Override
    public void enqueue(QueueDetail q, RqueueMessage m) {}

    @Override
    public void enqueueWithDelay(QueueDetail q, RqueueMessage m, long delayMs) {}

    @Override
    public List<RqueueMessage> pop(QueueDetail q, String consumerName, int batch, Duration wait) {
      int idx = popCalls.getAndIncrement();
      if (idx < popResponses.size()) {
        return popResponses.get(idx);
      }
      // mimic short wait so tests don't hot-spin
      try {
        Thread.sleep(20);
      } catch (InterruptedException ie) {
        Thread.currentThread().interrupt();
      }
      return Collections.emptyList();
    }

    @Override
    public boolean ack(QueueDetail q, RqueueMessage m) {
      ackd.add(m);
      return true;
    }

    @Override
    public boolean nack(QueueDetail q, RqueueMessage m, long retryDelayMs) {
      nackd.add(m);
      nackDelays.add(retryDelayMs);
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

  private QueueStateMgr stateMgr() {
    // queueStateMgr is non-static inner; the poller treats null as "never paused"
    return null;
  }

  @Test
  void dispatchesBatchAndAcksEach() throws Exception {
    StringHandler bean = new StringHandler();
    RqueueMessage m1 = rqMessage("a");
    RqueueMessage m2 = rqMessage("b");
    RqueueMessage m3 = rqMessage("c");
    FakeBroker broker = new FakeBroker(Collections.singletonList(Arrays.asList(m1, m2, m3)));

    BrokerMessagePoller poller = new BrokerMessagePoller(
        broker,
        queueDetail(),
        "consumer-1",
        handlerMethodFor(bean),
        converter,
        new FixedTaskExecutionBackOff(100L, 5),
        stateMgr(),
        4,
        Duration.ofMillis(50));

    ExecutorService es = Executors.newSingleThreadExecutor();
    es.submit(poller);
    waitFor(() -> bean.received.size() == 3, 2000);
    poller.stop();
    es.shutdown();
    assertTrue(es.awaitTermination(2, TimeUnit.SECONDS));

    assertEquals(Arrays.asList("a", "b", "c"), bean.received);
    assertEquals(3, broker.ackd.size());
    assertEquals(0, broker.nackd.size());
  }

  @Test
  void nacksOnHandlerException() throws Exception {
    StringHandler bean = new StringHandler();
    bean.throwOnInvoke = true;
    RqueueMessage m1 = rqMessage("oops");
    FakeBroker broker = new FakeBroker(Collections.singletonList(Collections.singletonList(m1)));

    BrokerMessagePoller poller = new BrokerMessagePoller(
        broker,
        queueDetail(),
        "consumer-1",
        handlerMethodFor(bean),
        converter,
        new FixedTaskExecutionBackOff(250L, 5),
        stateMgr(),
        4,
        Duration.ofMillis(50));

    ExecutorService es = Executors.newSingleThreadExecutor();
    es.submit(poller);
    waitFor(() -> broker.nackd.size() == 1, 2000);
    poller.stop();
    es.shutdown();
    assertTrue(es.awaitTermination(2, TimeUnit.SECONDS));

    assertEquals(0, broker.ackd.size());
    assertEquals(1, broker.nackd.size());
    Long delay = broker.nackDelays.peek();
    assertTrue(delay != null && delay > 0L, "expected non-zero retry delay, got " + delay);
  }

  @Test
  void stopsCleanlyOnStopSignal() throws Exception {
    StringHandler bean = new StringHandler();
    FakeBroker broker = new FakeBroker(Collections.emptyList()); // always empty
    BrokerMessagePoller poller = new BrokerMessagePoller(
        broker,
        queueDetail(),
        "consumer-1",
        handlerMethodFor(bean),
        converter,
        new FixedTaskExecutionBackOff(100L, 5),
        stateMgr(),
        4,
        Duration.ofMillis(20));

    CountDownLatch done = new CountDownLatch(1);
    Thread t = new Thread(() -> {
      poller.run();
      done.countDown();
    });
    t.start();
    Thread.sleep(80);
    assertTrue(poller.isRunning());
    poller.stop();
    assertTrue(done.await(2, TimeUnit.SECONDS), "poller did not exit run() after stop()");
    assertFalse(poller.isRunning());
  }

  private static void waitFor(java.util.function.BooleanSupplier cond, long timeoutMs)
      throws InterruptedException {
    long deadline = System.currentTimeMillis() + timeoutMs;
    while (System.currentTimeMillis() < deadline) {
      if (cond.getAsBoolean()) {
        return;
      }
      Thread.sleep(20);
    }
  }
}
