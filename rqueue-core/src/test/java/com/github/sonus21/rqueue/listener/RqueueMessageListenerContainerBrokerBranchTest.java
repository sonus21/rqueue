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
package com.github.sonus21.rqueue.listener;

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
import com.github.sonus21.rqueue.service.RqueueMessageMetadataService;
import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
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
class RqueueMessageListenerContainerBrokerBranchTest extends TestBase {

  @Mock
  private RedisConnectionFactory redisConnectionFactory;

  @Mock
  private ApplicationEventPublisher applicationEventPublisher;

  @Mock
  private RqueueMessageTemplate rqueueMessageTemplate;

  @Mock
  private RqueueSystemConfigDao rqueueSystemConfigDao;

  @Mock
  private RqueueMessageMetadataService rqueueMessageMetadataService;

  @Mock
  private RqueueWebConfig rqueueWebConfig;

  @Mock
  private RqueueLockManager rqueueLockManager;

  private RqueueBeanProvider beanProvider;
  private RqueueMessageHandler messageHandler;

  static class BrokerListener {
    final AtomicInteger received = new AtomicInteger();

    @RqueueListener(value = "broker-q1", consumerName = "consumer-A")
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
    applicationContext.registerSingleton("brokerListener", BrokerListener.class);
    messageHandler = new RqueueMessageHandler(new DefaultRqueueMessageConverter());
    messageHandler.setApplicationContext(applicationContext);
    messageHandler.afterPropertiesSet();
  }

  /** Counting broker that records pop calls but never returns messages. */
  static class CountingBroker implements MessageBroker, AutoCloseable {
    final AtomicInteger popCalls = new AtomicInteger();
    final AtomicBoolean closed = new AtomicBoolean();
    volatile Duration lastWait;
    private final Capabilities caps;

    CountingBroker(Capabilities caps) {
      this.caps = caps;
    }

    @Override
    public void enqueue(QueueDetail q, RqueueMessage m) {}

    @Override
    public void enqueueWithDelay(QueueDetail q, RqueueMessage m, long delayMs) {}

    @Override
    public List<RqueueMessage> pop(QueueDetail q, String consumerName, int batch, Duration wait) {
      popCalls.incrementAndGet();
      lastWait = wait;
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
      return caps;
    }

    @Override
    public void close() {
      closed.set(true);
    }
  }

  @Test
  void brokerWithPrimaryHandlerDispatchUsesNormalStartQueuePath() throws Exception {
    EndpointRegistry.delete();
    // Capabilities with usesPrimaryHandlerDispatch=true (e.g. NATS after the refactor).
    CountingBroker broker = new CountingBroker(new Capabilities(true, false, false, true, true, true));
    TrackingContainer container = new TrackingContainer(messageHandler);
    container.setMessageBroker(broker);
    container.afterPropertiesSet();
    container.start();
    try {
      // All brokers now go through the normal startQueue/startGroup path.
      assertTrue(
          container.startQueueCalled.get() || container.startGroupCalled.get(),
          "startQueue or startGroup should be invoked for any broker using primary handler"
              + " dispatch");
    } finally {
      container.stop();
      container.destroy();
    }
    assertTrue(broker.closed.get(), "AutoCloseable broker should be closed on destroy");
  }

  @Test
  void redisDefaultsBrokerAlsoUsesNormalStartQueuePath() throws Exception {
    EndpointRegistry.delete();
    CountingBroker broker = new CountingBroker(Capabilities.REDIS_DEFAULTS);
    TrackingContainer container = new TrackingContainer(messageHandler);
    container.setMessageBroker(broker);
    container.afterPropertiesSet();
    container.start();
    try {
      // broker-q1 has no priority group, so startQueue is expected.
      assertTrue(
          container.startQueueCalled.get() || container.startGroupCalled.get(),
          "legacy Redis-side wiring should run for REDIS_DEFAULTS capabilities");
      assertFalse(
          container.startBrokerPollersCalled.get(),
          "startBrokerPollers no longer exists; flag must remain false");
    } finally {
      container.stop();
      container.destroy();
    }
  }

  @Test
  void pollerForwardsPollingIntervalAsBrokerFetchWait() throws Exception {
    EndpointRegistry.delete();
    CountingBroker broker = new CountingBroker(new Capabilities(true, false, false, true, true, true));
    RqueueMessageListenerContainer container =
        new RqueueMessageListenerContainer(messageHandler, rqueueMessageTemplate);
    container.rqueueBeanProvider = beanProvider;
    container.setMessageBroker(broker);
    long pollingInterval = 137L;
    container.setPollingInterval(pollingInterval);
    container.afterPropertiesSet();
    container.start();
    try {
      // Wait for the poller to issue at least one pop call.
      long deadline = System.currentTimeMillis() + 2000;
      while (broker.popCalls.get() == 0 && System.currentTimeMillis() < deadline) {
        Thread.sleep(20);
      }
    } finally {
      container.stop();
      container.destroy();
    }
    assertTrue(broker.popCalls.get() > 0, "poller should have issued at least one pop call");
    Duration wait = broker.lastWait;
    assertNotNull(wait, "broker should have received a wait duration");
    assertFalse(wait.isZero(), "wait must not be Duration.ZERO; should match pollingInterval");
    assertTrue(
        wait.toMillis() == pollingInterval,
        "wait should equal the configured pollingInterval (got " + wait + ")");
  }

  private class TrackingContainer extends RqueueMessageListenerContainer {
    final AtomicBoolean startBrokerPollersCalled = new AtomicBoolean();
    final AtomicBoolean startQueueCalled = new AtomicBoolean();
    final AtomicBoolean startGroupCalled = new AtomicBoolean();

    TrackingContainer(RqueueMessageHandler handler) {
      super(handler, rqueueMessageTemplate);
      this.rqueueBeanProvider = beanProvider;
    }

    @Override
    protected void startQueue(String pollerKey, QueueDetail queueDetail) {
      startQueueCalled.set(true);
      // Do not actually start the poller; it would need a real broker.
    }

    @Override
    protected void startGroup(String groupName, List<QueueDetail> queueDetails) {
      startGroupCalled.set(true);
    }
  }
}
