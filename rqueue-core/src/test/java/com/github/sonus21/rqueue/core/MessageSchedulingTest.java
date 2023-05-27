/*
 * Copyright (c) 2022-2023 Sonu Kumar
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
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;

import com.github.sonus21.TestBase;
import com.github.sonus21.rqueue.CoreUnitTest;
import com.github.sonus21.rqueue.config.RqueueConfig;
import com.github.sonus21.rqueue.config.RqueueSchedulerConfig;
import com.github.sonus21.rqueue.core.ProcessingQueueMessageSchedulerTest.ProcessingQTestMessageScheduler;
import com.github.sonus21.rqueue.listener.QueueDetail;
import com.github.sonus21.rqueue.models.event.RqueueBootstrapEvent;
import com.github.sonus21.rqueue.utils.Constants;
import com.github.sonus21.rqueue.utils.TestUtils;
import com.github.sonus21.rqueue.utils.ThreadUtils;
import com.github.sonus21.rqueue.utils.TimeoutUtils;
import com.github.sonus21.test.TestTaskScheduler;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
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
import org.springframework.data.redis.connection.DefaultMessage;
import org.springframework.data.redis.connection.Message;
import org.springframework.data.redis.core.RedisCallback;
import org.springframework.data.redis.core.RedisTemplate;

@CoreUnitTest
@SuppressWarnings("unchecked")
class MessageSchedulingTest extends TestBase {

  @InjectMocks
  private final ProcessingQTestMessageScheduler messageScheduler = new ProcessingQTestMessageScheduler();
  private final String queue = "queue";
  private final QueueDetail queueDetail = TestUtils.createQueueDetail(queue);
  @Mock
  private RqueueSchedulerConfig rqueueSchedulerConfig;
  @Mock
  private RqueueConfig rqueueConfig;
  @Mock
  private RedisTemplate<String, Long> redisTemplate;
  @Mock
  private RqueueRedisListenerContainerFactory rqueueRedisListenerContainerFactory;

  @BeforeEach
  public void init() {
    MockitoAnnotations.openMocks(this);
    EndpointRegistry.delete();
    EndpointRegistry.register(queueDetail);
    doReturn(1).when(rqueueSchedulerConfig).getProcessingMessageThreadPoolSize();
    doReturn(200L).when(rqueueSchedulerConfig).getScheduledMessageTimeIntervalInMilli();
    doReturn(true).when(rqueueSchedulerConfig).isAutoStart();
    doReturn(true).when(rqueueSchedulerConfig).isEnabled();
    doReturn(true).when(rqueueSchedulerConfig).isRedisEnabled();
  }


  @Test
  void multipleTasksAreRunningForTheSameQueue() throws Exception {
    try (MockedStatic<ThreadUtils> threadUtils = Mockito.mockStatic(ThreadUtils.class)) {
      AtomicInteger counter = new AtomicInteger(0);
      doAnswer(invocation -> {
        counter.incrementAndGet();
        return System.currentTimeMillis();
      }).when(redisTemplate).execute(any(RedisCallback.class));
      TestTaskScheduler scheduler = new TestTaskScheduler();
      threadUtils.when(() -> ThreadUtils.createTaskScheduler(1, "processingQueueMsgScheduler-", 60))
          .thenReturn(scheduler);
      messageScheduler.onApplicationEvent(new RqueueBootstrapEvent("Test", true));
      waitFor(() -> counter.get() >= 2, "scripts are getting executed");
      sleep(10);
      messageScheduler.destroy();
      assertEquals(1, scheduler.submittedTasks());
    }
  }

  @Test
  void taskShouldBeScheduledOnFailure() throws Exception {
    try (MockedStatic<ThreadUtils> threadUtils = Mockito.mockStatic(ThreadUtils.class)) {
      doReturn(10000L).when(rqueueSchedulerConfig).getMaxMessageMoverDelay();
      doReturn(100L).when(rqueueSchedulerConfig).minMessageMoveDelay();
      AtomicInteger counter = new AtomicInteger(0);
      doAnswer(invocation -> {
        counter.incrementAndGet();
        throw new RedisSystemException("Something is not correct",
            new NullPointerException("oops!"));
      }).when(redisTemplate).execute(any(RedisCallback.class));
      TestTaskScheduler scheduler = new TestTaskScheduler();
      threadUtils.when(() -> ThreadUtils.createTaskScheduler(1, "processingQueueMsgScheduler-", 60))
          .thenReturn(scheduler);
      messageScheduler.onApplicationEvent(new RqueueBootstrapEvent("Test", true));
      waitFor(() -> counter.get() >= 3, "scripts are getting executed");
      sleep(10);
      messageScheduler.destroy();
      assertEquals(1, scheduler.submittedTasks());
    }
  }

  @Test
  void continuousTaskFailure() throws Exception {
    try (MockedStatic<ThreadUtils> threadUtils = Mockito.mockStatic(ThreadUtils.class)) {
      doReturn(500L).when(rqueueSchedulerConfig).getMaxMessageMoverDelay();
      doReturn(100L).when(rqueueSchedulerConfig).minMessageMoveDelay();
      AtomicInteger counter = new AtomicInteger(0);
      doAnswer(
          invocation -> {
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
          })
          .when(redisTemplate)
          .execute(any(RedisCallback.class));
      TestTaskScheduler scheduler = new TestTaskScheduler();
      threadUtils
          .when(() -> ThreadUtils.createTaskScheduler(1, "processingQueueMsgScheduler-", 60))
          .thenReturn(scheduler);
      messageScheduler.onApplicationEvent(new RqueueBootstrapEvent("Test", true));
      waitFor(() -> counter.get() >= 5, "scripts are getting executed");
      sleep(10);
      messageScheduler.destroy();
      assertEquals(1, scheduler.submittedTasks());
    }
  }

}
