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
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.github.sonus21.TestBase;
import com.github.sonus21.rqueue.CoreUnitTest;
import com.github.sonus21.rqueue.annotation.RqueueListener;
import com.github.sonus21.rqueue.common.RqueueLockManager;
import com.github.sonus21.rqueue.config.RqueueConfig;
import com.github.sonus21.rqueue.config.RqueueWebConfig;
import com.github.sonus21.rqueue.core.DefaultRqueueMessageConverter;
import com.github.sonus21.rqueue.core.EndpointRegistry;
import com.github.sonus21.rqueue.core.RqueueBeanProvider;
import com.github.sonus21.rqueue.core.RqueueMessage;
import com.github.sonus21.rqueue.core.RqueueMessageTemplate;
import com.github.sonus21.rqueue.core.spi.Capabilities;
import com.github.sonus21.rqueue.core.spi.MessageBroker;
import com.github.sonus21.rqueue.dao.RqueueSystemConfigDao;
import com.github.sonus21.rqueue.web.service.RqueueMessageMetadataService;
import java.time.Duration;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.context.support.StaticApplicationContext;
import org.springframework.data.redis.connection.RedisConnectionFactory;

@CoreUnitTest
class RqueueMessageListenerContainerPriorityTest extends TestBase {

  @Mock private RedisConnectionFactory redisConnectionFactory;
  @Mock private ApplicationEventPublisher applicationEventPublisher;
  @Mock private RqueueMessageTemplate rqueueMessageTemplate;
  @Mock private RqueueSystemConfigDao rqueueSystemConfigDao;
  @Mock private RqueueMessageMetadataService rqueueMessageMetadataService;
  @Mock private RqueueWebConfig rqueueWebConfig;
  @Mock private RqueueLockManager rqueueLockManager;

  private RqueueBeanProvider beanProvider;
  private RqueueMessageHandler messageHandler;

  static class PrioritizedListener {
    final AtomicInteger received = new AtomicInteger();

    @RqueueListener(value = "prio-q1", priority = "high=10,low=2", consumerName = "consumer-A")
    public void onMessage(String payload) {
      received.incrementAndGet();
    }
  }

  @BeforeEach
  void setUp() {
    MockitoAnnotations.openMocks(this);
    RqueueConfig rqueueConfig = new RqueueConfig(redisConnectionFactory, null, true, 1);
    beanProvider = new RqueueBeanProvider();
    beanProvider.setRqueueConfig(rqueueConfig);
    beanProvider.setRqueueSystemConfigDao(rqueueSystemConfigDao);
    beanProvider.setApplicationEventPublisher(applicationEventPublisher);
    beanProvider.setRqueueMessageTemplate(rqueueMessageTemplate);
    beanProvider.setRqueueMessageMetadataService(rqueueMessageMetadataService);
    beanProvider.setRqueueWebConfig(rqueueWebConfig);
    beanProvider.setRqueueLockManager(rqueueLockManager);
    StaticApplicationContext applicationContext = new StaticApplicationContext();
    applicationContext.registerSingleton("prioritizedListener", PrioritizedListener.class);
    messageHandler = new RqueueMessageHandler(new DefaultRqueueMessageConverter());
    messageHandler.setApplicationContext(applicationContext);
    messageHandler.afterPropertiesSet();
  }

  /**
   * Capability-gated broker that records pop calls per (priority, consumerName) and enqueues
   * with priority. Tracks routing decisions for assertions.
   */
  static class PriorityRecordingBroker implements MessageBroker, AutoCloseable {
    final ConcurrentHashMap<String, AtomicInteger> popCallsByKey = new ConcurrentHashMap<>();
    final ConcurrentLinkedQueue<String[]> enqueueRouting = new ConcurrentLinkedQueue<>();

    @Override
    public void enqueue(QueueDetail q, RqueueMessage m) {
      enqueueRouting.add(new String[] {q.getName(), null});
    }

    @Override
    public void enqueue(QueueDetail q, String priority, RqueueMessage m) {
      enqueueRouting.add(new String[] {q.getName(), priority});
    }

    @Override
    public void enqueueWithDelay(QueueDetail q, RqueueMessage m, long delayMs) {}

    @Override
    public List<RqueueMessage> pop(QueueDetail q, String consumerName, int batch, Duration wait) {
      // unrouted pop falls through to here for the default (no-priority) overload
      return pop(q, null, consumerName, batch, wait);
    }

    @Override
    public List<RqueueMessage> pop(
        QueueDetail q, String priority, String consumerName, int batch, Duration wait) {
      String key = q.getName() + "::" + (priority == null ? "_" : priority) + "::" + consumerName;
      popCallsByKey.computeIfAbsent(key, k -> new AtomicInteger()).incrementAndGet();
      try {
        Thread.sleep(20);
      } catch (InterruptedException ie) {
        Thread.currentThread().interrupt();
      }
      return Collections.emptyList();
    }

    @Override
    public boolean ack(QueueDetail q, RqueueMessage m) {
      return true;
    }

    @Override
    public boolean nack(QueueDetail q, RqueueMessage m, long retryDelayMs) {
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

    @Override
    public void close() {}
  }

  @Test
  void priorityQueueSpawnsOnePollerPerPriorityWithSuffixedConsumerName() throws Exception {
    EndpointRegistry.delete();
    PriorityRecordingBroker broker = new PriorityRecordingBroker();
    TrackingContainer container = new TrackingContainer(messageHandler);
    container.setMessageBroker(broker);
    container.afterPropertiesSet();
    container.start();
    try {
      List<BrokerMessagePoller> pollers = container.getBrokerPollersForTesting();
      assertEquals(2, pollers.size(), "expected one poller per priority entry");

      Set<String> consumerNames = new HashSet<>();
      Set<String> priorities = new HashSet<>();
      for (BrokerMessagePoller p : pollers) {
        consumerNames.add(p.getConsumerName());
        priorities.add(p.getPriority());
      }
      assertTrue(consumerNames.contains("consumer-A-high"));
      assertTrue(consumerNames.contains("consumer-A-low"));
      assertTrue(priorities.contains("high"));
      assertTrue(priorities.contains("low"));
      assertFalse(priorities.contains(null), "no priority-less poller expected");

      // Wait briefly for at least one priority-aware pop call.
      long deadline = System.currentTimeMillis() + 2000;
      while (System.currentTimeMillis() < deadline && broker.popCallsByKey.isEmpty()) {
        Thread.sleep(20);
      }
      // Both priorities should have invoked pop with their suffixed consumer name.
      assertNotNull(broker.popCallsByKey.get("prio-q1::high::consumer-A-high"));
      assertNotNull(broker.popCallsByKey.get("prio-q1::low::consumer-A-low"));
    } finally {
      container.stop();
      container.destroy();
    }
  }

  @Test
  void messageBrokerDefaultEnqueueDelegatesToUnsuffixedOverload() {
    // Verifies the SPI default contract: backends that don't override the priority-aware
    // overload (e.g. Redis) automatically delegate to enqueue(qd, msg). This is the additive
    // backwards-compatibility guarantee for the new default method.
    final java.util.concurrent.atomic.AtomicInteger plain = new java.util.concurrent.atomic.AtomicInteger();
    MessageBroker broker = new MessageBroker() {
      @Override
      public void enqueue(QueueDetail q, RqueueMessage m) {
        plain.incrementAndGet();
      }
      @Override
      public void enqueueWithDelay(QueueDetail q, RqueueMessage m, long delayMs) {}
      @Override
      public List<RqueueMessage> pop(QueueDetail q, String consumerName, int batch, Duration wait) {
        return Collections.emptyList();
      }
      @Override
      public boolean ack(QueueDetail q, RqueueMessage m) { return true; }
      @Override
      public boolean nack(QueueDetail q, RqueueMessage m, long retryDelayMs) { return true; }
      @Override
      public long moveExpired(QueueDetail q, long now, int batch) { return 0; }
      @Override
      public List<RqueueMessage> peek(QueueDetail q, long offset, long count) {
        return Collections.emptyList();
      }
      @Override
      public long size(QueueDetail q) { return 0; }
      @Override
      public AutoCloseable subscribe(String channel, Consumer<String> handler) { return () -> {}; }
      @Override
      public void publish(String channel, String payload) {}
      @Override
      public Capabilities capabilities() { return Capabilities.REDIS_DEFAULTS; }
    };
    QueueDetail qd = QueueDetail.builder()
        .name("q1")
        .queueName("__rq::queue::q1")
        .processingQueueName("__rq::pq::q1")
        .completedQueueName("__rq::cq::q1")
        .scheduledQueueName("__rq::sq::q1")
        .processingQueueChannelName("__rq::ch::q1")
        .scheduledQueueChannelName("__rq::sch::q1")
        .visibilityTimeout(30000)
        .numRetry(3)
        .priority(Collections.emptyMap())
        .build();
    RqueueMessage msg = RqueueMessage.builder().id("x").queueName("q1").message("p").build();
    broker.enqueue(qd, "high", msg);
    broker.enqueue(qd, msg);
    assertEquals(2, plain.get(), "default priority overload should delegate to enqueue(q, m)");
  }

  private class TrackingContainer extends RqueueMessageListenerContainer {
    TrackingContainer(RqueueMessageHandler handler) {
      super(handler, rqueueMessageTemplate);
      this.rqueueBeanProvider = beanProvider;
    }

    @Override
    protected void startQueue(String queueName, QueueDetail queueDetail) {
      // no-op for Redis path; this test only exercises broker pollers.
    }

    @Override
    protected void startGroup(String groupName, java.util.List<QueueDetail> queueDetails) {
      // no-op
    }
  }

}
