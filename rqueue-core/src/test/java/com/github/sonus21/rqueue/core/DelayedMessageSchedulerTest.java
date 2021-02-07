/*
 *  Copyright 2021 Sonu Kumar
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *         https://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and limitations under the License.
 *
 */

package com.github.sonus21.rqueue.core;

import static com.github.sonus21.rqueue.utils.TimeoutUtils.sleep;
import static com.github.sonus21.rqueue.utils.TimeoutUtils.waitFor;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;

import com.github.sonus21.TestBase;
import com.github.sonus21.rqueue.CoreUnitTest;
import com.github.sonus21.rqueue.config.RqueueSchedulerConfig;
import com.github.sonus21.rqueue.listener.QueueDetail;
import com.github.sonus21.rqueue.models.event.RqueueBootstrapEvent;
import com.github.sonus21.rqueue.utils.TestUtils;
import com.github.sonus21.rqueue.utils.ThreadUtils;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Vector;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.springframework.data.redis.connection.DefaultMessage;
import org.springframework.data.redis.connection.MessageListener;
import org.springframework.data.redis.core.RedisCallback;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.listener.ChannelTopic;
import org.springframework.data.redis.listener.RedisMessageListenerContainer;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;

@CoreUnitTest
@SuppressWarnings("unchecked")
class DelayedMessageSchedulerTest extends TestBase {

  @Mock
  private RqueueSchedulerConfig rqueueSchedulerConfig;
  @Mock
  private RqueueRedisListenerContainerFactory rqueueRedisListenerContainerFactory;
  @Mock
  private RedisMessageListenerContainer redisMessageListenerContainer;
  @Mock
  private RedisTemplate<String, Long> redisTemplate;

  @InjectMocks
  private final TestMessageScheduler messageScheduler = new TestMessageScheduler();

  private final String slowQueue = "slow-queue";
  private final String fastQueue = "fast-queue";
  private final QueueDetail slowQueueDetail = TestUtils.createQueueDetail(slowQueue);
  private final QueueDetail fastQueueDetail = TestUtils.createQueueDetail(fastQueue);

  @BeforeEach
  public void init() {
    MockitoAnnotations.openMocks(this);
    EndpointRegistry.delete();
    EndpointRegistry.register(fastQueueDetail);
    EndpointRegistry.register(slowQueueDetail);
  }

  @Test
  void getChannelName() {
    assertEquals(
        slowQueueDetail.getDelayedQueueChannelName(), messageScheduler.getChannelName(slowQueue));
  }

  @Test
  void getZsetName() {
    assertEquals(slowQueueDetail.getDelayedQueueName(), messageScheduler.getZsetName(slowQueue));
  }

  @Test
  void getNextScheduleTime() {
    long currentTime = System.currentTimeMillis();
    doReturn(5000L).when(rqueueSchedulerConfig).getDelayedMessageTimeInterval();
    assertThat(
        messageScheduler.getNextScheduleTime(slowQueue, null),
        greaterThanOrEqualTo(currentTime + 5000L));
    assertThat(
        messageScheduler.getNextScheduleTime(fastQueue, currentTime + 1000L),
        greaterThanOrEqualTo(currentTime + 5000L));
  }

  @Test
  void afterPropertiesSetWithEmptyQueSet() throws Exception {
    EndpointRegistry.delete();
    messageScheduler.onApplicationEvent(new RqueueBootstrapEvent("Test", true));
    assertNull(FieldUtils.readField(messageScheduler, "scheduler", true));
    assertNull(FieldUtils.readField(messageScheduler, "queueRunningState", true));
    assertNull(FieldUtils.readField(messageScheduler, "queueNameToScheduledTask", true));
    assertNull(FieldUtils.readField(messageScheduler, "channelNameToQueueName", true));
    assertNull(FieldUtils.readField(messageScheduler, "queueNameToLastMessageSeenTime", true));
  }

  @Test
  void start() throws Exception {
    doReturn(1).when(rqueueSchedulerConfig).getDelayedMessageThreadPoolSize();
    doReturn(true).when(rqueueSchedulerConfig).isAutoStart();
    doReturn(true).when(rqueueSchedulerConfig).isRedisEnabled();
    doReturn(1000L).when(rqueueSchedulerConfig).getDelayedMessageTimeInterval();
    doReturn(redisMessageListenerContainer)
        .when(rqueueRedisListenerContainerFactory)
        .getContainer();
    messageScheduler.onApplicationEvent(new RqueueBootstrapEvent("Test", true));
    Map<String, Boolean> queueRunningState =
        (Map<String, Boolean>) FieldUtils.readField(messageScheduler, "queueRunningState", true);
    assertEquals(2, queueRunningState.size());
    assertTrue(queueRunningState.get(slowQueue));
    assertEquals(
        2, ((Map) FieldUtils.readField(messageScheduler, "queueNameToScheduledTask", true)).size());
    assertEquals(
        2, ((Map) FieldUtils.readField(messageScheduler, "channelNameToQueueName", true)).size());
    Thread.sleep(500L);
    messageScheduler.destroy();
  }

  @Test
  void startAddsChannelToMessageListener() throws Exception {
    doReturn(1000L).when(rqueueSchedulerConfig).getDelayedMessageTimeInterval();
    doReturn(1).when(rqueueSchedulerConfig).getDelayedMessageThreadPoolSize();
    doReturn(true).when(rqueueSchedulerConfig).isAutoStart();
    doReturn(true).when(rqueueSchedulerConfig).isRedisEnabled();
    doReturn(redisMessageListenerContainer)
        .when(rqueueRedisListenerContainerFactory)
        .getContainer();
    doNothing()
        .when(redisMessageListenerContainer)
        .addMessageListener(
            any(), eq(new ChannelTopic(slowQueueDetail.getDelayedQueueChannelName())));
    doNothing()
        .when(redisMessageListenerContainer)
        .addMessageListener(
            any(), eq(new ChannelTopic(fastQueueDetail.getDelayedQueueChannelName())));
    messageScheduler.onApplicationEvent(new RqueueBootstrapEvent("Test", true));
    Thread.sleep(500L);
    messageScheduler.destroy();
  }

  @Test
  void stop() throws Exception {
    doReturn(1).when(rqueueSchedulerConfig).getDelayedMessageThreadPoolSize();
    doReturn(true).when(rqueueSchedulerConfig).isAutoStart();
    doReturn(true).when(rqueueSchedulerConfig).isRedisEnabled();
    doReturn(1000L).when(rqueueSchedulerConfig).getDelayedMessageTimeInterval();
    doReturn(redisMessageListenerContainer)
        .when(rqueueRedisListenerContainerFactory)
        .getContainer();
    messageScheduler.onApplicationEvent(new RqueueBootstrapEvent("Test", true));
    Thread.sleep(500L);
    messageScheduler.onApplicationEvent(new RqueueBootstrapEvent("Test", false));
    Map<String, Boolean> queueRunningState =
        (Map<String, Boolean>) FieldUtils.readField(messageScheduler, "queueRunningState", true);
    assertEquals(2, queueRunningState.size());
    assertFalse(queueRunningState.get(slowQueue));
    assertEquals(
        2, ((Map) FieldUtils.readField(messageScheduler, "channelNameToQueueName", true)).size());
    assertTrue(
        ((Map) FieldUtils.readField(messageScheduler, "queueNameToScheduledTask", true)).isEmpty());
    messageScheduler.destroy();
  }

  @Test
  void destroy() throws Exception {
    doReturn(1).when(rqueueSchedulerConfig).getDelayedMessageThreadPoolSize();
    doReturn(true).when(rqueueSchedulerConfig).isAutoStart();
    doReturn(true).when(rqueueSchedulerConfig).isRedisEnabled();
    doReturn(1000L).when(rqueueSchedulerConfig).getDelayedMessageTimeInterval();
    doReturn(redisMessageListenerContainer)
        .when(rqueueRedisListenerContainerFactory)
        .getContainer();
    TestThreadPoolScheduler scheduler = new TestThreadPoolScheduler();
    try (MockedStatic<ThreadUtils> threadUtils = Mockito.mockStatic(ThreadUtils.class)) {
      threadUtils
          .when(() -> ThreadUtils.createTaskScheduler(1, "delayedMessageScheduler-", 60))
          .thenReturn(scheduler);
      messageScheduler.onApplicationEvent(new RqueueBootstrapEvent("Test", true));
      Thread.sleep(500L);
      messageScheduler.destroy();
      Map<String, Boolean> queueRunningState =
          (Map<String, Boolean>) FieldUtils.readField(messageScheduler, "queueRunningState", true);
      assertEquals(2, queueRunningState.size());
      assertFalse(queueRunningState.get(slowQueue));
      assertTrue(
          ((Map) FieldUtils.readField(messageScheduler, "queueNameToScheduledTask", true))
              .isEmpty());
      assertTrue(scheduler.shutdown);
    }
  }

  @Test
  void startSubmitsTask() throws Exception {
    doReturn(1).when(rqueueSchedulerConfig).getDelayedMessageThreadPoolSize();
    doReturn(true).when(rqueueSchedulerConfig).isAutoStart();
    doReturn(true).when(rqueueSchedulerConfig).isRedisEnabled();
    doReturn(redisMessageListenerContainer)
        .when(rqueueRedisListenerContainerFactory)
        .getContainer();
    TestThreadPoolScheduler scheduler = new TestThreadPoolScheduler();
    try (MockedStatic<ThreadUtils> threadUtils = Mockito.mockStatic(ThreadUtils.class)) {
      threadUtils
          .when(() -> ThreadUtils.createTaskScheduler(1, "delayedMessageScheduler-", 60))
          .thenReturn(scheduler);
      messageScheduler.onApplicationEvent(new RqueueBootstrapEvent("Test", true));
      assertTrue(scheduler.tasks.size() >= 1);
      messageScheduler.destroy();
    }
  }

  @Test
  void startSubmitsTaskAndThatGetsExecuted() throws Exception {
    doReturn(1).when(rqueueSchedulerConfig).getDelayedMessageThreadPoolSize();
    doReturn(true).when(rqueueSchedulerConfig).isAutoStart();
    doReturn(true).when(rqueueSchedulerConfig).isRedisEnabled();
    doReturn(1000L).when(rqueueSchedulerConfig).getDelayedMessageTimeInterval();
    doReturn(redisMessageListenerContainer)
        .when(rqueueRedisListenerContainerFactory)
        .getContainer();
    AtomicInteger counter = new AtomicInteger(0);
    doAnswer(
            invocation -> {
              counter.incrementAndGet();
              return null;
            })
        .when(redisTemplate)
        .execute(any(RedisCallback.class));
    messageScheduler.onApplicationEvent(new RqueueBootstrapEvent("Test", true));
    waitFor(() -> counter.get() >= 1, "scripts are getting executed");
    messageScheduler.destroy();
  }

  @Test
  void onCompletionOfExistingTaskNewTaskIsSubmitted() throws Exception {
    try (MockedStatic<ThreadUtils> threadUtils = Mockito.mockStatic(ThreadUtils.class)) {
      doReturn(1).when(rqueueSchedulerConfig).getDelayedMessageThreadPoolSize();
      doReturn(true).when(rqueueSchedulerConfig).isAutoStart();
      doReturn(true).when(rqueueSchedulerConfig).isRedisEnabled();
      doReturn(1000L).when(rqueueSchedulerConfig).getDelayedMessageTimeInterval();
      doReturn(redisMessageListenerContainer)
          .when(rqueueRedisListenerContainerFactory)
          .getContainer();
      AtomicInteger counter = new AtomicInteger(0);
      doAnswer(
              invocation -> {
                counter.incrementAndGet();
                return null;
              })
          .when(redisTemplate)
          .execute(any(RedisCallback.class));
      TestThreadPoolScheduler scheduler = new TestThreadPoolScheduler();
      threadUtils
          .when(() -> ThreadUtils.createTaskScheduler(1, "delayedMessageScheduler-", 60))
          .thenReturn(scheduler);
      messageScheduler.onApplicationEvent(new RqueueBootstrapEvent("Test", true));
      waitFor(() -> counter.get() >= 1, "scripts are getting executed");
      sleep(10);
      messageScheduler.destroy();
      assertTrue(scheduler.tasks.size() >= 2);
    }
  }

  @Test
  void onMessageListenerTest() throws Exception {
    doReturn(5000L).when(rqueueSchedulerConfig).getDelayedMessageTimeInterval();
    doReturn(1).when(rqueueSchedulerConfig).getDelayedMessageThreadPoolSize();
    doReturn(true).when(rqueueSchedulerConfig).isAutoStart();
    doReturn(true).when(rqueueSchedulerConfig).isRedisEnabled();
    doReturn(redisMessageListenerContainer)
        .when(rqueueRedisListenerContainerFactory)
        .getContainer();
    messageScheduler.onApplicationEvent(new RqueueBootstrapEvent("Test", true));

    MessageListener messageListener =
        (MessageListener) FieldUtils.readField(messageScheduler, "messageSchedulerListener", true);
    // invalid channel
    messageListener.onMessage(new DefaultMessage(slowQueue.getBytes(), "312".getBytes()), null);
    Thread.sleep(50);
    assertEquals(2, messageScheduler.scheduleList.stream().filter(e -> !e).count());

    // invalid body
    messageListener.onMessage(
        new DefaultMessage(
            slowQueueDetail.getDelayedQueueChannelName().getBytes(), "sss".getBytes()),
        null);
    Thread.sleep(50);
    assertEquals(2, messageScheduler.scheduleList.stream().filter(e -> !e).count());

    // both are correct
    messageListener.onMessage(
        new DefaultMessage(
            slowQueueDetail.getDelayedQueueChannelName().getBytes(),
            String.valueOf(System.currentTimeMillis()).getBytes()),
        null);
    Thread.sleep(50);
    assertEquals(3, messageScheduler.scheduleList.stream().filter(e -> !e).count());
    messageScheduler.destroy();
  }

  static class TestThreadPoolScheduler extends ThreadPoolTaskScheduler {

    private static final long serialVersionUID = 3617860362304703358L;
    boolean shutdown = false;
    List<Future<?>> tasks = new Vector<>();

    public TestThreadPoolScheduler() {
      setPoolSize(1);
      afterPropertiesSet();
    }

    @Override
    public Future<?> submit(Runnable r) {
      Future<?> f = super.submit(r);
      tasks.add(f);
      return f;
    }

    @Override
    public ScheduledFuture<?> schedule(Runnable r, Instant instant) {
      ScheduledFuture<?> f = super.schedule(r, instant);
      tasks.add(f);
      return f;
    }

    @Override
    public void shutdown() {
      super.shutdown();
      shutdown = true;
    }
  }

  static class TestMessageScheduler extends DelayedMessageScheduler {
    List<Boolean> scheduleList;

    TestMessageScheduler() {
      this.scheduleList = new Vector<>();
    }

    @Override
    protected synchronized void schedule(String queueName, Long startTime, boolean forceSchedule) {
      super.schedule(queueName, startTime, forceSchedule);
      this.scheduleList.add(forceSchedule);
    }
  }
}
