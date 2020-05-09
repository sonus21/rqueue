/*
 * Copyright 2020 Sonu Kumar
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

import static com.github.sonus21.rqueue.utils.TimeoutUtils.sleep;
import static com.github.sonus21.rqueue.utils.TimeoutUtils.waitFor;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;

import com.github.sonus21.rqueue.config.RqueueSchedulerConfig;
import com.github.sonus21.rqueue.listener.QueueDetail;
import com.github.sonus21.rqueue.models.event.RqueueBootstrapEvent;
import com.github.sonus21.rqueue.utils.TestUtils;
import com.github.sonus21.rqueue.utils.ThreadUtils;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Vector;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;
import org.mockito.quality.Strictness;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.springframework.data.redis.connection.DefaultMessage;
import org.springframework.data.redis.connection.MessageListener;
import org.springframework.data.redis.core.RedisCallback;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.listener.ChannelTopic;
import org.springframework.data.redis.listener.RedisMessageListenerContainer;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;

@RunWith(PowerMockRunner.class)
@PrepareForTest(fullyQualifiedNames = {"com.github.sonus21.rqueue.utils.ThreadUtils"})
public class DelayedMessageSchedulerTest {
  @Rule public MockitoRule mockito = MockitoJUnit.rule().strictness(Strictness.STRICT_STUBS);
  @Mock private RqueueSchedulerConfig rqueueSchedulerConfig;
  @Mock private RqueueRedisListenerContainerFactory rqueueRedisListenerContainerFactory;
  @Mock private RedisMessageListenerContainer redisMessageListenerContainer;
  @Mock private RedisTemplate<String, Long> redisTemplate;

  @InjectMocks private TestMessageScheduler messageScheduler = new TestMessageScheduler();

  private String slowQueue = "slow-queue";
  private String fastQueue = "fast-queue";
  private QueueDetail slowQueueDetail = TestUtils.createQueueDetail(slowQueue);
  private QueueDetail fastQueueDetail = TestUtils.createQueueDetail(fastQueue);

  @Before
  public void init() {
    MockitoAnnotations.initMocks(this);
    QueueRegistry.delete();
    QueueRegistry.register(fastQueueDetail);
    QueueRegistry.register(slowQueueDetail);
  }

  @Test
  public void getChannelName() {
    assertEquals(
        slowQueueDetail.getDelayedQueueChannelName(), messageScheduler.getChannelName(slowQueue));
  }

  @Test
  public void getZsetName() {
    assertEquals(slowQueueDetail.getDelayedQueueName(), messageScheduler.getZsetName(slowQueue));
  }

  @Test
  public void getNextScheduleTime() {
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
  public void afterPropertiesSetWithEmptyQueSet() throws Exception {
    QueueRegistry.delete();
    messageScheduler.onApplicationEvent(new RqueueBootstrapEvent("Test", true));
    assertNull(FieldUtils.readField(messageScheduler, "scheduler", true));
    assertNull(FieldUtils.readField(messageScheduler, "queueRunningState", true));
    assertNull(FieldUtils.readField(messageScheduler, "queueNameToScheduledTask", true));
    assertNull(FieldUtils.readField(messageScheduler, "channelNameToQueueName", true));
    assertNull(FieldUtils.readField(messageScheduler, "queueNameToLastMessageSeenTime", true));
  }

  @Test
  public void start() throws Exception {
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
  public void startAddsChannelToMessageListener() throws Exception {
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
  public void stop() throws Exception {
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
  public void destroy() throws Exception {
    doReturn(1).when(rqueueSchedulerConfig).getDelayedMessageThreadPoolSize();
    doReturn(true).when(rqueueSchedulerConfig).isAutoStart();
    doReturn(true).when(rqueueSchedulerConfig).isRedisEnabled();
    doReturn(1000L).when(rqueueSchedulerConfig).getDelayedMessageTimeInterval();
    doReturn(redisMessageListenerContainer)
        .when(rqueueRedisListenerContainerFactory)
        .getContainer();
    TestThreadPoolScheduler scheduler = new TestThreadPoolScheduler();
    PowerMockito.stub(
            PowerMockito.method(
                ThreadUtils.class, "createTaskScheduler", Integer.TYPE, String.class, Integer.TYPE))
        .toReturn(scheduler);
    messageScheduler.onApplicationEvent(new RqueueBootstrapEvent("Test", true));
    Thread.sleep(500L);
    messageScheduler.destroy();
    Map<String, Boolean> queueRunningState =
        (Map<String, Boolean>) FieldUtils.readField(messageScheduler, "queueRunningState", true);
    assertEquals(2, queueRunningState.size());
    assertFalse(queueRunningState.get(slowQueue));
    assertTrue(
        ((Map) FieldUtils.readField(messageScheduler, "queueNameToScheduledTask", true)).isEmpty());
    assertTrue(scheduler.shutdown);
  }

  @Test
  public void startSubmitsTask() throws Exception {
    doReturn(1).when(rqueueSchedulerConfig).getDelayedMessageThreadPoolSize();
    doReturn(true).when(rqueueSchedulerConfig).isAutoStart();
    doReturn(true).when(rqueueSchedulerConfig).isRedisEnabled();
    doReturn(redisMessageListenerContainer)
        .when(rqueueRedisListenerContainerFactory)
        .getContainer();
    TestThreadPoolScheduler scheduler = new TestThreadPoolScheduler();
    PowerMockito.stub(
            PowerMockito.method(
                ThreadUtils.class, "createTaskScheduler", Integer.TYPE, String.class, Integer.TYPE))
        .toReturn(scheduler);
    messageScheduler.onApplicationEvent(new RqueueBootstrapEvent("Test", true));
    assertTrue(scheduler.tasks.size() >= 1);
    messageScheduler.destroy();
  }

  @Test
  public void startSubmitsTaskAndThatGetsExecuted() throws Exception {
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
  public void onCompletionOfExistingTaskNewTaskIsSubmitted() throws Exception {
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
    PowerMockito.stub(
            PowerMockito.method(
                ThreadUtils.class, "createTaskScheduler", Integer.TYPE, String.class, Integer.TYPE))
        .toReturn(scheduler);
    messageScheduler.onApplicationEvent(new RqueueBootstrapEvent("Test", true));
    waitFor(() -> counter.get() >= 1, "scripts are getting executed");
    sleep(10);
    messageScheduler.destroy();
    assertTrue(scheduler.tasks.size() >= 2);
  }

  @Test
  public void onMessageListenerTest() throws Exception {
    doReturn(1000L).when(rqueueSchedulerConfig).getDelayedMessageTimeInterval();
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
    Thread.sleep(100);
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
      this.scheduleList = new ArrayList<>();
    }

    @Override
    protected synchronized void schedule(String queueName, Long startTime, boolean forceSchedule) {
      super.schedule(queueName, startTime, forceSchedule);
      this.scheduleList.add(forceSchedule);
    }
  }
}
