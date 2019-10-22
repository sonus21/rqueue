/*
 * Copyright (c)  2019-2019, Sonu Kumar
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *       https://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */

package com.github.sonus21.rqueue.listener;

import static com.github.sonus21.rqueue.utils.TimeUtil.waitFor;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;

import com.github.sonus21.rqueue.annotation.RqueueListener;
import com.github.sonus21.rqueue.core.RqueueMessage;
import com.github.sonus21.rqueue.core.RqueueMessageTemplate;
import com.github.sonus21.rqueue.core.StringMessageTemplate;
import com.github.sonus21.rqueue.utils.Constants;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import lombok.Getter;
import org.junit.Test;
import org.springframework.context.support.StaticApplicationContext;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

public class RqueueMessageListenerContainerTest {
  private static final String slowQueue = "slow-queue";
  private static final String fastQueue = "fast-queue";
  private RqueueMessageListenerContainer container =
      new RqueueMessageListenerContainer(
          mock(RqueueMessageHandler.class),
          mock(RqueueMessageTemplate.class),
          mock(StringMessageTemplate.class));

  @Test
  public void setDelayedQueueSleepTime() {
    container.setDelayedQueueSleepTime(100L);
    assertEquals(100L, container.getDelayedQueueSleepTime());
  }

  @Test
  public void setMaxWorkerWaitTime() {
    container.setMaxWorkerWaitTime(20000L);
    assertEquals(20000L, container.getMaxWorkerWaitTime());
  }

  @Test
  public void setBeanName() {
    container.setBeanName("TestBean");
    assertEquals("TestBean", container.getBeanName());
  }

  @Test
  public void setMaxNumWorkers() {
    container.setMaxNumWorkers(1000);
    assertEquals(Integer.valueOf(1000), container.getMaxNumWorkers());
  }

  @Test
  public void setBackOffTime() {
    container.setBackOffTime(1000L);
    assertEquals(1000L, container.getBackoffTime());
  }

  @Test
  public void setAutoStartup() {
    container.setAutoStartup(false);
    assertFalse(container.isAutoStartup());
    container.setAutoStartup(true);
    assertTrue(container.isAutoStartup());
  }

  @Test
  public void setTaskExecutor() {
    assertNull(container.getTaskExecutor());
    ThreadPoolTaskExecutor asyncTaskExecutor = new ThreadPoolTaskExecutor();
    asyncTaskExecutor.setThreadNamePrefix("testExecutor");
    container.setTaskExecutor(asyncTaskExecutor);
    assertEquals(
        "testExecutor",
        ((ThreadPoolTaskExecutor) container.getTaskExecutor()).getThreadNamePrefix());
  }

  @Test
  public void testPhaseSetting() {
    assertEquals(Integer.MAX_VALUE, container.getPhase());
    container.setPhase(100);
    assertEquals(100, container.getPhase());
  }

  @Test
  public void checkDoStartMethodIsCalled() throws Exception {
    StubMessageListenerContainer container = new StubMessageListenerContainer();
    container.afterPropertiesSet();
    container.start();
    assertTrue(container.isDoStartMethodIsCalled());
    assertFalse(container.isDestroyMethodIsCalled());
    assertFalse(container.isDoStopMethodIsCalled());
  }

  @Test
  public void checkDoStopMethodIsCalled() throws Exception {
    StubMessageListenerContainer container = new StubMessageListenerContainer();
    container.afterPropertiesSet();
    container.start();
    container.stop();
    assertTrue(container.isDoStopMethodIsCalled());
  }

  @Test
  public void checkDoDestroyMethodIsCalled() throws Exception {
    StubMessageListenerContainer container = new StubMessageListenerContainer();
    container.afterPropertiesSet();
    container.start();
    container.stop();
    container.destroy();
    assertTrue(container.isDestroyMethodIsCalled());
  }

  @Test
  public void checkDoStopMethodIsCalledWithRunnable() throws Exception {
    StubMessageListenerContainer container = new StubMessageListenerContainer();
    CountDownLatch count = new CountDownLatch(1);
    container.afterPropertiesSet();
    container.start();
    container.stop(count::countDown);

    container.destroy();
    assertTrue(container.isDestroyMethodIsCalled());
    assertEquals(0, count.getCount());
  }

  @Test
  public void testMessagesAreGettingFetchedFromRedis() throws Exception {
    RqueueMessageTemplate rqueueMessageTemplate = mock(RqueueMessageTemplate.class);
    StringMessageTemplate stringMessageTemplate = mock(StringMessageTemplate.class);

    StaticApplicationContext applicationContext = new StaticApplicationContext();
    applicationContext.registerSingleton("messageHandler", RqueueMessageHandler.class);
    applicationContext.registerSingleton("slowMessageListener", SlowMessageListener.class);
    applicationContext.registerSingleton("fastMessageListener", FastMessageListener.class);
    RqueueMessageHandler messageHandler =
        applicationContext.getBean("messageHandler", RqueueMessageHandler.class);
    messageHandler.setApplicationContext(applicationContext);
    messageHandler.afterPropertiesSet();

    RqueueMessageListenerContainer container =
        new RqueueMessageListenerContainer(
            messageHandler, rqueueMessageTemplate, stringMessageTemplate);
    AtomicInteger fastQueueCounter = new AtomicInteger(0);
    AtomicInteger zsetCounter = new AtomicInteger(0);
    doAnswer(
            invocation -> {
              fastQueueCounter.incrementAndGet();
              return null;
            })
        .when(rqueueMessageTemplate)
        .lpop(fastQueue);

    doAnswer(
            invocation -> {
              zsetCounter.incrementAndGet();
              return null;
            })
        .when(rqueueMessageTemplate)
        .getFirstFromZset(Constants.getZsetName(slowQueue));
    container.afterPropertiesSet();
    container.start();
    waitFor(() -> fastQueueCounter.get() > 1, "fastQueue message call");
    waitFor(() -> zsetCounter.get() > 1, "slowQueue message call");
    container.stop();
    container.doDestroy();
  }

  @Test
  public void testMessagesAreGettingMovedToQueue() throws Exception {
    RqueueMessageTemplate rqueueMessageTemplate = mock(RqueueMessageTemplate.class);
    StringMessageTemplate stringMessageTemplate = mock(StringMessageTemplate.class);

    StaticApplicationContext applicationContext = new StaticApplicationContext();
    applicationContext.registerSingleton("messageHandler", RqueueMessageHandler.class);
    applicationContext.registerSingleton("slowMessageListener", SlowMessageListener.class);
    RqueueMessageHandler messageHandler =
        applicationContext.getBean("messageHandler", RqueueMessageHandler.class);
    messageHandler.setApplicationContext(applicationContext);
    messageHandler.afterPropertiesSet();

    RqueueMessageListenerContainer container =
        new RqueueMessageListenerContainer(
            messageHandler, rqueueMessageTemplate, stringMessageTemplate);

    AtomicInteger zsetCounter = new AtomicInteger(0);
    AtomicInteger messageCounter = new AtomicInteger(0);

    doReturn(true).when(stringMessageTemplate).putIfAbsent(anyString(), anyLong(), any());
    doAnswer(
            invocation -> {
              messageCounter.incrementAndGet();
              return null;
            })
        .when(rqueueMessageTemplate)
        .add(eq(slowQueue), any(RqueueMessage.class));

    doAnswer(
            invocation -> {
              if (zsetCounter.get() < 10) {
                zsetCounter.incrementAndGet();
                return new RqueueMessage(slowQueue, "This is a test", 3, -100L);
              } else if (zsetCounter.get() < 20) {
                zsetCounter.incrementAndGet();
                return new RqueueMessage(slowQueue, "This is a test", 3, 1000L);
              }
              return null;
            })
        .when(rqueueMessageTemplate)
        .getFirstFromZset(Constants.getZsetName(slowQueue));

    container.afterPropertiesSet();
    container.start();
    waitFor(() -> zsetCounter.get() == 20, "Generate all slowQueue messages");
    waitFor(() -> messageCounter.get() == 10, "10 messages should be moved to slowQueue");
    container.stop();
    container.doDestroy();
  }

  @Test
  public void testMessageHandlersAreInvoked() throws Exception {
    RqueueMessageTemplate rqueueMessageTemplate = mock(RqueueMessageTemplate.class);
    StringMessageTemplate stringMessageTemplate = mock(StringMessageTemplate.class);

    StaticApplicationContext applicationContext = new StaticApplicationContext();
    applicationContext.registerSingleton("messageHandler", RqueueMessageHandler.class);
    applicationContext.registerSingleton("slowMessageListener", SlowMessageListener.class);
    applicationContext.registerSingleton("fastMessageListener", FastMessageListener.class);
    RqueueMessageHandler messageHandler =
        applicationContext.getBean("messageHandler", RqueueMessageHandler.class);
    messageHandler.setApplicationContext(applicationContext);
    messageHandler.afterPropertiesSet();

    RqueueMessageListenerContainer container =
        new RqueueMessageListenerContainer(
            messageHandler, rqueueMessageTemplate, stringMessageTemplate);
    FastMessageListener fastMessageListener =
        applicationContext.getBean("fastMessageListener", FastMessageListener.class);
    SlowMessageListener slowMessageListener =
        applicationContext.getBean("slowMessageListener", SlowMessageListener.class);

    AtomicInteger slowQueueCounter = new AtomicInteger(0);
    AtomicInteger fastQueueCounter = new AtomicInteger(0);
    String fastQueueMessage = "This is fast queue";
    String slowQueueMessage = "This is slow queue";
    doAnswer(
            invocation -> {
              if (slowQueueCounter.get() == 0) {
                slowQueueCounter.incrementAndGet();
                return new RqueueMessage(slowQueue, slowQueueMessage, null, null);
              }
              return null;
            })
        .when(rqueueMessageTemplate)
        .lpop(slowQueue);

    doAnswer(
            invocation -> {
              if (fastQueueCounter.get() == 0) {
                fastQueueCounter.incrementAndGet();
                return new RqueueMessage(fastQueue, fastQueueMessage, null, null);
              }
              return null;
            })
        .when(rqueueMessageTemplate)
        .lpop(fastQueue);
    container.afterPropertiesSet();
    container.start();
    waitFor(() -> slowQueueCounter.get() == 1, "slowQueue message fetch");
    waitFor(() -> fastQueueCounter.get() == 1, "fastQueue message fetch");
    waitFor(
        () -> fastQueueMessage.equals(fastMessageListener.getLastMessage()),
        "FastQueue message consumer call");
    waitFor(
        () -> slowQueueMessage.equals(slowMessageListener.getLastMessage()),
        "SlowQueue message consumer call");
    container.stop();
    container.doDestroy();
  }

  @SuppressWarnings({"UnusedDeclaration"})
  @Getter
  private static class StubMessageListenerContainer extends RqueueMessageListenerContainer {
    private boolean destroyMethodIsCalled = false;
    private boolean doStartMethodIsCalled = false;
    private boolean doStopMethodIsCalled = false;

    StubMessageListenerContainer() {
      super(
          mock(RqueueMessageHandler.class),
          mock(RqueueMessageTemplate.class),
          mock(StringMessageTemplate.class));
    }

    @Override
    protected void doStart() {
      doStartMethodIsCalled = true;
    }

    @Override
    protected void doStop() {
      doStopMethodIsCalled = true;
    }

    @Override
    protected void doDestroy() {
      destroyMethodIsCalled = true;
    }
  }

  @SuppressWarnings({"UnusedDeclaration"})
  @Getter
  private static class SlowMessageListener {
    private String lastMessage;

    @RqueueListener(value = slowQueue, delayedQueue = "true")
    public void onMessage(String message) {
      this.lastMessage = message;
    }
  }

  @SuppressWarnings({"UnusedDeclaration"})
  @Getter
  private static class FastMessageListener {
    private String lastMessage;

    @RqueueListener(fastQueue)
    public void onMessage(String message) {
      this.lastMessage = message;
    }
  }
}
