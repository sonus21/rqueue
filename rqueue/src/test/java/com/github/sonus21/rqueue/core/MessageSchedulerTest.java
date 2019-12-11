/*
 * Copyright 2019 Sonu Kumar
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.github.sonus21.rqueue.core;

import static com.github.sonus21.rqueue.utils.TimeUtil.waitFor;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;

import com.github.sonus21.rqueue.listener.ConsumerQueueDetail;
import com.github.sonus21.rqueue.listener.RqueueMessageListenerContainer;
import com.github.sonus21.rqueue.utils.QueueInfo;
import java.time.Instant;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Vector;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.mockito.junit.MockitoJUnitRunner;
import org.springframework.data.redis.connection.DefaultMessage;
import org.springframework.data.redis.connection.MessageListener;
import org.springframework.data.redis.core.RedisCallback;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.listener.ChannelTopic;
import org.springframework.data.redis.listener.RedisMessageListenerContainer;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;

@RunWith(MockitoJUnitRunner.StrictStubs.class)
public class MessageSchedulerTest {
  private int poolSize = 1;
  @Mock private RqueueMessageListenerContainer rqueueMessageListenerContainer;
  @Mock private RedisMessageListenerContainer redisMessageListenerContainer;
  @Mock private RedisTemplate<String, Long> redisTemplate;

  @InjectMocks
  private TestMessageScheduler messageScheduler =
      new TestMessageScheduler(redisTemplate, poolSize, true);

  private String slowQueue = "slow-queue";
  private String fastQueue = "fast-queue";
  private ConsumerQueueDetail slowQueueDetail = new ConsumerQueueDetail(slowQueue, -1, "", true);
  private ConsumerQueueDetail fastQueueDetail = new ConsumerQueueDetail(fastQueue, -1, "", false);
  private Map<String, ConsumerQueueDetail> queueNameToQueueDetail = new HashMap<>();

  @Before
  public void init() {
    MockitoAnnotations.initMocks(this);
    queueNameToQueueDetail.put(slowQueue, slowQueueDetail);
    queueNameToQueueDetail.put(fastQueue, fastQueueDetail);
  }

  @Test
  public void getChannelName() {
    assertEquals(QueueInfo.getChannelName(slowQueue), messageScheduler.getChannelName(slowQueue));
  }

  @Test
  public void getZsetName() {
    assertEquals(QueueInfo.getTimeQueueName(slowQueue), messageScheduler.getZsetName(slowQueue));
  }

  @Test
  public void isQueueValid() {
    assertTrue(messageScheduler.isQueueValid(slowQueueDetail));
    assertFalse(messageScheduler.isQueueValid(fastQueueDetail));
  }

  @Test
  public void getNextScheduleTime() {
    long currentTime = System.currentTimeMillis();
    assertEquals(currentTime + 5000L, messageScheduler.getNextScheduleTime(currentTime, null));
    assertEquals(
        currentTime + 5000L,
        messageScheduler.getNextScheduleTime(currentTime, currentTime + 1000L));
  }

  @Test
  public void afterPropertiesSetWithEmptyQueSet() throws Exception {
    doReturn(Collections.emptyMap()).when(rqueueMessageListenerContainer).getRegisteredQueues();
    messageScheduler.afterPropertiesSet();
    assertNull(FieldUtils.readField(messageScheduler, "scheduler", true));
    assertTrue(((Map) FieldUtils.readField(messageScheduler, "queueRunningState", true)).isEmpty());
    assertTrue(
        ((Map) FieldUtils.readField(messageScheduler, "queueNameToScheduledTask", true)).isEmpty());
    assertTrue(
        ((Map) FieldUtils.readField(messageScheduler, "channelNameToQueueName", true)).isEmpty());
    assertTrue(
        ((Map) FieldUtils.readField(messageScheduler, "queueNameToZsetName", true)).isEmpty());
    assertTrue(
        ((Map) FieldUtils.readField(messageScheduler, "queueNameToLastMessageSeenTime", true))
            .isEmpty());
  }

  @Test
  public void afterPropertiesSet() throws Exception {
    doReturn(queueNameToQueueDetail).when(rqueueMessageListenerContainer).getRegisteredQueues();
    messageScheduler.afterPropertiesSet();
    assertNotNull(FieldUtils.readField(messageScheduler, "scheduler", true));
    Map<String, Boolean> queueRunningState =
        (Map<String, Boolean>) FieldUtils.readField(messageScheduler, "queueRunningState", true);
    assertEquals(1, queueRunningState.size());
    assertFalse(queueRunningState.get(slowQueue));
    assertEquals(
        0, ((Map) FieldUtils.readField(messageScheduler, "queueNameToScheduledTask", true)).size());
    assertEquals(
        0, ((Map) FieldUtils.readField(messageScheduler, "channelNameToQueueName", true)).size());
    assertEquals(
        0, ((Map) FieldUtils.readField(messageScheduler, "queueNameToZsetName", true)).size());
    messageScheduler.destroy();
  }

  @Test
  public void start() throws Exception {
    doReturn(queueNameToQueueDetail).when(rqueueMessageListenerContainer).getRegisteredQueues();
    messageScheduler.afterPropertiesSet();
    messageScheduler.start();
    Map<String, Boolean> queueRunningState =
        (Map<String, Boolean>) FieldUtils.readField(messageScheduler, "queueRunningState", true);
    assertEquals(1, queueRunningState.size());
    assertTrue(queueRunningState.get(slowQueue));
    assertEquals(
        1, ((Map) FieldUtils.readField(messageScheduler, "queueNameToScheduledTask", true)).size());
    assertEquals(
        1, ((Map) FieldUtils.readField(messageScheduler, "channelNameToQueueName", true)).size());
    assertEquals(
        1, ((Map) FieldUtils.readField(messageScheduler, "queueNameToZsetName", true)).size());
    assertTrue(messageScheduler.isRunning());
    Thread.sleep(500L);
    messageScheduler.destroy();
  }

  @Test
  public void startAddsChannelToMessageListener() throws Exception {
    doReturn(queueNameToQueueDetail).when(rqueueMessageListenerContainer).getRegisteredQueues();
    doNothing()
        .when(redisMessageListenerContainer)
        .addMessageListener(any(), eq(new ChannelTopic(QueueInfo.getChannelName(slowQueue))));
    messageScheduler.afterPropertiesSet();
    messageScheduler.start();
    Thread.sleep(500L);
    messageScheduler.destroy();
  }

  @Test
  public void stop() throws Exception {
    doReturn(queueNameToQueueDetail).when(rqueueMessageListenerContainer).getRegisteredQueues();
    messageScheduler.afterPropertiesSet();
    messageScheduler.start();
    Thread.sleep(500L);
    messageScheduler.stop();
    Map<String, Boolean> queueRunningState =
        (Map<String, Boolean>) FieldUtils.readField(messageScheduler, "queueRunningState", true);
    assertEquals(1, queueRunningState.size());
    assertFalse(queueRunningState.get(slowQueue));
    assertEquals(
        1, ((Map) FieldUtils.readField(messageScheduler, "channelNameToQueueName", true)).size());
    assertEquals(
        1, ((Map) FieldUtils.readField(messageScheduler, "queueNameToZsetName", true)).size());
    assertTrue(
        ((Map) FieldUtils.readField(messageScheduler, "queueNameToScheduledTask", true)).isEmpty());
    messageScheduler.destroy();
  }

  @Test
  public void destroy() throws Exception {
    doReturn(queueNameToQueueDetail).when(rqueueMessageListenerContainer).getRegisteredQueues();
    messageScheduler.afterPropertiesSet();
    TestTaskScheduler scheduler = new TestTaskScheduler();
    scheduler.setPoolSize(1);
    scheduler.afterPropertiesSet();

    FieldUtils.writeField(messageScheduler, "scheduler", scheduler, true);
    messageScheduler.start();
    Thread.sleep(500L);
    messageScheduler.destroy();
    Map<String, Boolean> queueRunningState =
        (Map<String, Boolean>) FieldUtils.readField(messageScheduler, "queueRunningState", true);
    assertEquals(1, queueRunningState.size());
    assertFalse(queueRunningState.get(slowQueue));
    assertTrue(
        ((Map) FieldUtils.readField(messageScheduler, "queueNameToScheduledTask", true)).isEmpty());
    assertTrue(scheduler.shutdown);
  }

  @Test
  public void startSubmitsTask() throws Exception {
    doReturn(queueNameToQueueDetail).when(rqueueMessageListenerContainer).getRegisteredQueues();
    messageScheduler.afterPropertiesSet();
    TestTaskScheduler scheduler = new TestTaskScheduler();
    scheduler.setPoolSize(1);
    scheduler.afterPropertiesSet();
    FieldUtils.writeField(messageScheduler, "scheduler", scheduler, true);
    messageScheduler.start();
    assertTrue(scheduler.tasks.size() >= 1);
    messageScheduler.destroy();
  }

  @Test
  public void startSubmitsTaskAndThatGetsExecuted() throws Exception {
    AtomicInteger counter = new AtomicInteger(0);
    doReturn(queueNameToQueueDetail).when(rqueueMessageListenerContainer).getRegisteredQueues();
    doAnswer(
            invocation -> {
              counter.incrementAndGet();
              return null;
            })
        .when(redisTemplate)
        .execute(any(RedisCallback.class));
    messageScheduler.afterPropertiesSet();
    messageScheduler.start();
    waitFor(() -> counter.get() >= 1, "scripts are getting executed");
    messageScheduler.destroy();
  }

  @Test
  public void onCompletionOfExistingTaskNewTaskIsSubmitted() throws Exception {
    AtomicInteger counter = new AtomicInteger(0);
    doReturn(queueNameToQueueDetail).when(rqueueMessageListenerContainer).getRegisteredQueues();
    doAnswer(
            invocation -> {
              counter.incrementAndGet();
              return null;
            })
        .when(redisTemplate)
        .execute(any(RedisCallback.class));
    messageScheduler.afterPropertiesSet();
    TestTaskScheduler scheduler = new TestTaskScheduler();
    scheduler.setPoolSize(1);
    scheduler.afterPropertiesSet();
    FieldUtils.writeField(messageScheduler, "scheduler", scheduler, true);
    messageScheduler.start();
    waitFor(() -> counter.get() >= 1, "scripts are getting executed");
    messageScheduler.destroy();
    assertTrue(scheduler.tasks.size() >= 2);
  }

  @Test
  public void onMessageListenerTest() throws Exception {
    doReturn(queueNameToQueueDetail).when(rqueueMessageListenerContainer).getRegisteredQueues();
    messageScheduler.afterPropertiesSet();
    messageScheduler.start();
    MessageListener messageListener =
        (MessageListener) FieldUtils.readField(messageScheduler, "messageSchedulerListener", true);
    // invalid channel
    messageListener.onMessage(new DefaultMessage(slowQueue.getBytes(), "312".getBytes()), null);
    assertEquals(1, messageScheduler.scheduleList.stream().filter(e -> !e).count());

    // invalid body
    messageListener.onMessage(
        new DefaultMessage(QueueInfo.getChannelName(slowQueue).getBytes(), "sss".getBytes()), null);
    assertEquals(1, messageScheduler.scheduleList.stream().filter(e -> !e).count());

    // both are correct
    messageListener.onMessage(
        new DefaultMessage(
            QueueInfo.getChannelName(slowQueue).getBytes(),
            String.valueOf(System.currentTimeMillis()).getBytes()),
        null);
    assertEquals(2, messageScheduler.scheduleList.stream().filter(e -> !e).count());
    messageScheduler.destroy();
  }

  static class TestTaskScheduler extends ThreadPoolTaskScheduler {

    private static final long serialVersionUID = 3617860362304703358L;
    boolean shutdown = false;
    List<Future<?>> tasks = new Vector<>();

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
    List<Boolean> scheduleList = new Vector<>();

    TestMessageScheduler(
        RedisTemplate<String, Long> redisTemplate, int poolSize, boolean scheduleTaskAtStartup) {
      super(redisTemplate, poolSize, scheduleTaskAtStartup);
    }

    @Override
    protected synchronized void schedule(
        String queueName, String zsetName, Long startTime, boolean forceSchedule) {
      super.schedule(queueName, zsetName, startTime, forceSchedule);
      scheduleList.add(forceSchedule);
    }
  }
}
