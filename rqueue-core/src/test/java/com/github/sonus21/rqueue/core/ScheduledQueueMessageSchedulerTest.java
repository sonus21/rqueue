/*
 * Copyright (c) 2019-2023 Sonu Kumar
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
import com.github.sonus21.rqueue.config.RqueueConfig;
import com.github.sonus21.rqueue.config.RqueueSchedulerConfig;
import com.github.sonus21.rqueue.listener.QueueDetail;
import com.github.sonus21.rqueue.models.event.RqueueBootstrapEvent;
import com.github.sonus21.rqueue.utils.Constants;
import com.github.sonus21.rqueue.utils.TestUtils;
import com.github.sonus21.rqueue.utils.ThreadUtils;
import com.github.sonus21.rqueue.utils.TimeoutUtils;
import com.github.sonus21.test.TestTaskScheduler;
import java.util.Map;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.springframework.data.redis.ClusterRedirectException;
import org.springframework.data.redis.RedisConnectionFailureException;
import org.springframework.data.redis.RedisSystemException;
import org.springframework.data.redis.TooManyClusterRedirectionsException;
import org.springframework.data.redis.core.RedisCallback;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.listener.ChannelTopic;

@CoreUnitTest
@SuppressWarnings("unchecked")
class ScheduledQueueMessageSchedulerTest extends TestBase {

  private final String slowQueue = "slow-queue";
  private final String fastQueue = "fast-queue";
  private final QueueDetail slowQueueDetail = TestUtils.createQueueDetail(slowQueue);
  private final QueueDetail fastQueueDetail = TestUtils.createQueueDetail(fastQueue);
  @Mock
  private RqueueSchedulerConfig rqueueSchedulerConfig;
  @Mock
  private RqueueConfig rqueueConfig;
  @Mock
  private RedisTemplate<String, Long> redisTemplate;
  @Mock
  private RqueueRedisListenerContainerFactory rqueueRedisListenerContainerFactory;
  @InjectMocks
  private TestScheduledQueueMessageScheduler messageScheduler;

  @BeforeEach
  public void init() {
    MockitoAnnotations.openMocks(this);
    EndpointRegistry.delete();
    EndpointRegistry.register(fastQueueDetail);
    EndpointRegistry.register(slowQueueDetail);
  }

  @Test
  void getChannelName() {
    assertEquals(slowQueueDetail.getScheduledQueueChannelName(),
        messageScheduler.getChannelName(slowQueue));
  }

  @Test
  void getZsetName() {
    assertEquals(slowQueueDetail.getScheduledQueueName(), messageScheduler.getZsetName(slowQueue));
  }

  @Test
  void getNextScheduleTime() {
    long currentTime = System.currentTimeMillis();
    doReturn(5000L).when(rqueueSchedulerConfig).getScheduledMessageTimeIntervalInMilli();
    assertThat(messageScheduler.getNextScheduleTime(slowQueue, currentTime, null),
        greaterThanOrEqualTo(currentTime + 5000L));
    assertThat(messageScheduler.getNextScheduleTime(fastQueue, currentTime, currentTime + 1000L),
        greaterThanOrEqualTo(currentTime + 5000L));
  }

  @Test
  void afterPropertiesSetWithEmptyQueSet() throws Exception {
    EndpointRegistry.delete();
    doReturn(true).when(rqueueSchedulerConfig).isEnabled();
    messageScheduler.onApplicationEvent(new RqueueBootstrapEvent("Test", true));
    assertNull(FieldUtils.readField(messageScheduler, "scheduler", true));
    assertNull(FieldUtils.readField(messageScheduler, "queueRunningState", true));
    assertNull(FieldUtils.readField(messageScheduler, "queueNameToScheduledTask", true));
    assertNull(FieldUtils.readField(messageScheduler, "queueNameToNextRunTime", true));
    assertNull(FieldUtils.readField(messageScheduler, "redisScheduleTriggerHandler", true));
  }

  @Test
  void start() throws Exception {
    doReturn(1).when(rqueueSchedulerConfig).getScheduledMessageThreadPoolSize();
    doReturn(true).when(rqueueSchedulerConfig).isAutoStart();
    doReturn(true).when(rqueueSchedulerConfig).isRedisEnabled();
    doReturn(true).when(rqueueSchedulerConfig).isEnabled();
    doReturn(1000L).when(rqueueSchedulerConfig).getScheduledMessageTimeIntervalInMilli();
    messageScheduler.onApplicationEvent(new RqueueBootstrapEvent("Test", true));
    Map<String, Boolean> queueRunningState = (Map<String, Boolean>) FieldUtils.readField(
        messageScheduler, "queueRunningState", true);
    assertEquals(2, queueRunningState.size());
    assertTrue(queueRunningState.get(slowQueue));
    assertEquals(2,
        ((Map) FieldUtils.readField(messageScheduler, "queueNameToScheduledTask", true)).size());
    TimeoutUtils.sleep(500L);
    messageScheduler.destroy();
  }

  @Test
  void startAddsChannelToMessageListener() throws Exception {
    doReturn(1000L).when(rqueueSchedulerConfig).getScheduledMessageTimeIntervalInMilli();
    doReturn(1).when(rqueueSchedulerConfig).getScheduledMessageThreadPoolSize();
    doReturn(true).when(rqueueSchedulerConfig).isAutoStart();
    doReturn(true).when(rqueueSchedulerConfig).isEnabled();
    doReturn(true).when(rqueueSchedulerConfig).isRedisEnabled();
    doNothing().when(rqueueRedisListenerContainerFactory).addMessageListener(any(),
        eq(new ChannelTopic(slowQueueDetail.getScheduledQueueChannelName())));
    doNothing().when(rqueueRedisListenerContainerFactory).addMessageListener(any(),
        eq(new ChannelTopic(fastQueueDetail.getScheduledQueueChannelName())));
    messageScheduler.onApplicationEvent(new RqueueBootstrapEvent("Test", true));
    TimeoutUtils.sleep(500L);
    messageScheduler.destroy();
  }

  @Test
  void stop() throws Exception {
    doReturn(1).when(rqueueSchedulerConfig).getScheduledMessageThreadPoolSize();
    doReturn(true).when(rqueueSchedulerConfig).isAutoStart();
    doReturn(true).when(rqueueSchedulerConfig).isEnabled();
    doReturn(true).when(rqueueSchedulerConfig).isRedisEnabled();
    doReturn(1000L).when(rqueueSchedulerConfig).getScheduledMessageTimeIntervalInMilli();
    messageScheduler.onApplicationEvent(new RqueueBootstrapEvent("Test", true));
    TimeoutUtils.sleep(500L);
    messageScheduler.onApplicationEvent(new RqueueBootstrapEvent("Test", false));
    Map<String, Boolean> queueRunningState = (Map<String, Boolean>) FieldUtils.readField(
        messageScheduler, "queueRunningState", true);
    assertEquals(2, queueRunningState.size());
    assertFalse(queueRunningState.get(slowQueue));
    assertEquals(2,
        ((Map) FieldUtils.readField(messageScheduler, "queueNameToNextRunTime", true)).size());
    assertTrue(
        ((Map) FieldUtils.readField(messageScheduler, "queueNameToScheduledTask", true)).isEmpty());
    messageScheduler.destroy();
  }

  @Test
  void destroy() throws Exception {
    doReturn(1).when(rqueueSchedulerConfig).getScheduledMessageThreadPoolSize();
    doReturn(true).when(rqueueSchedulerConfig).isAutoStart();
    doReturn(true).when(rqueueSchedulerConfig).isEnabled();
    doReturn(true).when(rqueueSchedulerConfig).isRedisEnabled();
    doReturn(1000L).when(rqueueSchedulerConfig).getScheduledMessageTimeIntervalInMilli();
    TestTaskScheduler scheduler = new TestTaskScheduler();
    try (MockedStatic<ThreadUtils> threadUtils = Mockito.mockStatic(ThreadUtils.class)) {
      threadUtils.when(() -> ThreadUtils.createTaskScheduler(1, "scheduledQueueMsgScheduler-", 60))
          .thenReturn(scheduler);
      messageScheduler.onApplicationEvent(new RqueueBootstrapEvent("Test", true));
      TimeoutUtils.sleep(500L);
      messageScheduler.destroy();
      Map<String, Boolean> queueRunningState = (Map<String, Boolean>) FieldUtils.readField(
          messageScheduler, "queueRunningState", true);
      assertEquals(2, queueRunningState.size());
      assertFalse(queueRunningState.get(slowQueue));
      assertTrue(((Map) FieldUtils.readField(messageScheduler, "queueNameToScheduledTask",
          true)).isEmpty());
      assertTrue(scheduler.shutdown);
    }
  }

  @Test
  void startSubmitsTask() throws Exception {
    doReturn(1).when(rqueueSchedulerConfig).getScheduledMessageThreadPoolSize();
    doReturn(true).when(rqueueSchedulerConfig).isAutoStart();
    doReturn(true).when(rqueueSchedulerConfig).isEnabled();
    doReturn(true).when(rqueueSchedulerConfig).isRedisEnabled();
    TestTaskScheduler scheduler = new TestTaskScheduler();
    try (MockedStatic<ThreadUtils> threadUtils = Mockito.mockStatic(ThreadUtils.class)) {
      threadUtils.when(() -> ThreadUtils.createTaskScheduler(1, "scheduledQueueMsgScheduler-", 60))
          .thenReturn(scheduler);
      messageScheduler.onApplicationEvent(new RqueueBootstrapEvent("Test", true));
      assertTrue(scheduler.submittedTasks() >= 1);
      messageScheduler.destroy();
    }
  }

  @Test
  void startSubmitsTaskAndThatGetsExecuted() throws Exception {
    doReturn(1).when(rqueueSchedulerConfig).getScheduledMessageThreadPoolSize();
    doReturn(true).when(rqueueSchedulerConfig).isAutoStart();
    doReturn(true).when(rqueueSchedulerConfig).isEnabled();
    doReturn(true).when(rqueueSchedulerConfig).isRedisEnabled();
    doReturn(1000L).when(rqueueSchedulerConfig).getScheduledMessageTimeIntervalInMilli();
    AtomicInteger counter = new AtomicInteger(0);
    doAnswer(invocation -> {
      counter.incrementAndGet();
      return null;
    }).when(redisTemplate).execute(any(RedisCallback.class));
    messageScheduler.onApplicationEvent(new RqueueBootstrapEvent("Test", true));
    waitFor(() -> counter.get() >= 1, "scripts are getting executed");
    messageScheduler.destroy();
  }

  @Test
  void onCompletionOfExistingTaskNewTaskShouldBeSubmitted() throws Exception {
    try (MockedStatic<ThreadUtils> threadUtils = Mockito.mockStatic(ThreadUtils.class)) {
      doReturn(1).when(rqueueSchedulerConfig).getScheduledMessageThreadPoolSize();
      doReturn(true).when(rqueueSchedulerConfig).isAutoStart();
      doReturn(true).when(rqueueSchedulerConfig).isEnabled();
      doReturn(true).when(rqueueSchedulerConfig).isRedisEnabled();
      doReturn(1000L).when(rqueueSchedulerConfig).getScheduledMessageTimeIntervalInMilli();
      AtomicInteger counter = new AtomicInteger(0);
      doAnswer(invocation -> {
        counter.incrementAndGet();
        return null;
      }).when(redisTemplate).execute(any(RedisCallback.class));
      TestTaskScheduler scheduler = new TestTaskScheduler();
      threadUtils.when(() -> ThreadUtils.createTaskScheduler(1, "scheduledQueueMsgScheduler-", 60))
          .thenReturn(scheduler);
      messageScheduler.onApplicationEvent(new RqueueBootstrapEvent("Test", true));
      waitFor(() -> counter.get() >= 1, "scripts are getting executed");
      sleep(10);
      messageScheduler.destroy();
      assertTrue(scheduler.submittedTasks() >= 2);
    }
  }

  @Test
  void taskShouldBeScheduledOnFailure() throws Exception {
    try (MockedStatic<ThreadUtils> threadUtils = Mockito.mockStatic(ThreadUtils.class)) {
      doReturn(1).when(rqueueSchedulerConfig).getScheduledMessageThreadPoolSize();
      doReturn(true).when(rqueueSchedulerConfig).isAutoStart();
      doReturn(true).when(rqueueSchedulerConfig).isEnabled();
      doReturn(true).when(rqueueSchedulerConfig).isRedisEnabled();
      doReturn(10000L).when(rqueueSchedulerConfig).getMaxMessageMoverDelay();
      AtomicInteger counter = new AtomicInteger(0);
      doAnswer(invocation -> {
        counter.incrementAndGet();
        throw new RedisSystemException("Something is not correct",
            new NullPointerException("oops!"));
      }).when(redisTemplate).execute(any(RedisCallback.class));
      TestTaskScheduler scheduler = new TestTaskScheduler();
      threadUtils.when(() -> ThreadUtils.createTaskScheduler(1, "scheduledQueueMsgScheduler-", 60))
          .thenReturn(scheduler);
      messageScheduler.onApplicationEvent(new RqueueBootstrapEvent("Test", true));
      waitFor(() -> counter.get() >= 1, "scripts are getting executed");
      sleep(10);
      messageScheduler.destroy();
      assertTrue(scheduler.submittedTasks() >= 2);
    }
  }

  @Test
  void continuousTaskFailTask() throws Exception {
    try (MockedStatic<ThreadUtils> threadUtils = Mockito.mockStatic(ThreadUtils.class)) {
      doReturn(1).when(rqueueSchedulerConfig).getScheduledMessageThreadPoolSize();
      doReturn(true).when(rqueueSchedulerConfig).isAutoStart();
      doReturn(true).when(rqueueSchedulerConfig).isEnabled();
      doReturn(true).when(rqueueSchedulerConfig).isRedisEnabled();
      doReturn(100L).when(rqueueSchedulerConfig).getMaxMessageMoverDelay();
      AtomicInteger counter = new AtomicInteger(0);
      doAnswer(invocation -> {
        int count = counter.incrementAndGet();
        if (count % 3 == 0) {
          throw new RedisSystemException("Something is not correct",
              new NullPointerException("oops!"));
        }
        if (count % 3 == 1) {
          throw new RedisConnectionFailureException("Unknown host");
        }
        throw new ClusterRedirectException(3, "localhost", 9004,
            new TooManyClusterRedirectionsException("too many redirects"));
      }).when(redisTemplate).execute(any(RedisCallback.class));
      TestTaskScheduler scheduler = new TestTaskScheduler();
      threadUtils.when(() -> ThreadUtils.createTaskScheduler(1, "scheduledQueueMsgScheduler-", 60))
          .thenReturn(scheduler);
      messageScheduler.onApplicationEvent(new RqueueBootstrapEvent("Test", true));
      waitFor(() -> counter.get() >= 10, "scripts are getting executed");
      sleep(10);
      messageScheduler.destroy();
    }
  }


  static class TestScheduledQueueMessageScheduler extends ScheduledQueueMessageScheduler {

    final AtomicInteger scheduleCounter;
    final AtomicInteger addTaskCounter;

    TestScheduledQueueMessageScheduler() {
      this.scheduleCounter = new AtomicInteger(0);
      this.addTaskCounter = new AtomicInteger(0);
    }

    @Override
    protected void schedule(String queueName) {
      scheduleCounter.incrementAndGet();
      super.schedule(queueName);
    }

    @Override
    protected Future<?> addTask(String queueName) {
      addTaskCounter.incrementAndGet();
      return super.addTask(queueName);
    }
  }
}
