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

import static org.junit.jupiter.api.Assertions.assertFalse;
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

  /** Capability-gated broker that records pop calls but never returns messages. */
  static class CountingBroker implements MessageBroker, AutoCloseable {
    final AtomicInteger popCalls = new AtomicInteger();
    final AtomicBoolean closed = new AtomicBoolean();
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
  void brokerBranchInvokesStartBrokerPollersAndSkipsRedisWiring() throws Exception {
    EndpointRegistry.delete();
    CountingBroker broker = new CountingBroker(new Capabilities(true, false, false, false));
    TrackingContainer container = new TrackingContainer(messageHandler);
    container.setMessageBroker(broker);
    container.afterPropertiesSet();
    container.start();
    try {
      assertTrue(container.startBrokerPollersCalled.get(), "startBrokerPollers should be called");
      assertFalse(
          container.startQueueCalled.get(),
          "Redis-side startQueue should NOT be called for broker path");
      assertFalse(
          container.startGroupCalled.get(),
          "Redis-side startGroup should NOT be called for broker path");
      // Wait briefly to ensure the poller actually got submitted and is calling pop.
      long deadline = System.currentTimeMillis() + 2000;
      while (System.currentTimeMillis() < deadline && broker.popCalls.get() == 0) {
        Thread.sleep(20);
      }
      assertTrue(broker.popCalls.get() > 0, "broker.pop should have been invoked at least once");
    } finally {
      container.stop();
      container.destroy();
    }
    assertTrue(broker.closed.get(), "AutoCloseable broker should be closed on destroy");
  }

  @Test
  void redisCapabilitiesUsesLegacyPathNotBrokerPollers() throws Exception {
    EndpointRegistry.delete();
    CountingBroker broker = new CountingBroker(Capabilities.REDIS_DEFAULTS);
    TrackingContainer container = new TrackingContainer(messageHandler);
    container.setMessageBroker(broker);
    container.afterPropertiesSet();
    container.start();
    try {
      assertFalse(
          container.startBrokerPollersCalled.get(),
          "broker pollers should not start when capabilities use primary handler dispatch");
      // Either startQueue or startGroup is invoked from the legacy path; exact one depends on
      // priority configuration. broker-q1 has no priority, so startQueue is expected.
      assertTrue(
          container.startQueueCalled.get() || container.startGroupCalled.get(),
          "legacy Redis-side wiring should run for REDIS_DEFAULTS capabilities");
    } finally {
      container.stop();
      container.destroy();
    }
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
    protected void startBrokerPollers() {
      startBrokerPollersCalled.set(true);
      super.startBrokerPollers();
    }

    @Override
    protected void startQueue(String queueName, QueueDetail queueDetail) {
      startQueueCalled.set(true);
      // Do not actually start the Redis-side poller; it would block on a real Redis.
    }

    @Override
    protected void startGroup(String groupName, List<QueueDetail> queueDetails) {
      startGroupCalled.set(true);
    }
  }
}
