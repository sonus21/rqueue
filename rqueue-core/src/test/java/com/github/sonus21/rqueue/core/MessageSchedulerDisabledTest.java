/*
 * Copyright (c) 2020-2023 Sonu Kumar
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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.Mockito.doReturn;

import com.github.sonus21.TestBase;
import com.github.sonus21.rqueue.CoreUnitTest;
import com.github.sonus21.rqueue.config.RqueueConfig;
import com.github.sonus21.rqueue.config.RqueueSchedulerConfig;
import com.github.sonus21.rqueue.listener.QueueDetail;
import com.github.sonus21.rqueue.models.event.RqueueBootstrapEvent;
import com.github.sonus21.rqueue.utils.TestUtils;
import com.github.sonus21.rqueue.utils.ThreadUtils;
import com.github.sonus21.test.TestTaskScheduler;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.springframework.data.redis.core.RedisTemplate;

@CoreUnitTest
class MessageSchedulerDisabledTest extends TestBase {

  @InjectMocks
  private final ScheduledQueueMessageScheduler messageScheduler =
      new ScheduledQueueMessageScheduler();

  private final String slowQueue = "slow-queue";
  private final QueueDetail slowQueueDetail = TestUtils.createQueueDetail(slowQueue);
  @Mock
  private RqueueSchedulerConfig rqueueSchedulerConfig;
  @Mock
  private RqueueConfig rqueueConfig;
  @Mock
  private RedisTemplate<String, Long> redisTemplate;

  @BeforeEach
  public void init() {
    MockitoAnnotations.openMocks(this);
    EndpointRegistry.delete();
    EndpointRegistry.register(slowQueueDetail);
  }

  @Test
  void startShouldSubmitsTaskWhenRedisIsDisabled() throws Exception {
    doReturn(1).when(rqueueSchedulerConfig).getScheduledMessageThreadPoolSize();
    doReturn(true).when(rqueueSchedulerConfig).isEnabled();
    doReturn(true).when(rqueueSchedulerConfig).isAutoStart();
    TestTaskScheduler scheduler = new TestTaskScheduler();
    try (MockedStatic<ThreadUtils> threadUtils = Mockito.mockStatic(ThreadUtils.class)) {
      threadUtils
          .when(() -> ThreadUtils.createTaskScheduler(1, "scheduledQueueMsgScheduler-", 60))
          .thenReturn(scheduler);
      messageScheduler.onApplicationEvent(new RqueueBootstrapEvent("Test", true));
      assertEquals(1, scheduler.submittedTasks());
      assertNull(messageScheduler.redisScheduleTriggerHandler);
      messageScheduler.destroy();
    }
  }

  @Test
  void afterPropertiesSetProducerMode() throws Exception {
    doReturn(true).when(rqueueConfig).isProducer();
    doReturn(true).when(rqueueSchedulerConfig).isEnabled();
    messageScheduler.onApplicationEvent(new RqueueBootstrapEvent("Test", true));
    assertNull(FieldUtils.readField(messageScheduler, "scheduler", true));
    assertNull(FieldUtils.readField(messageScheduler, "queueRunningState", true));
    assertNull(FieldUtils.readField(messageScheduler, "queueNameToScheduledTask", true));
    assertNull(FieldUtils.readField(messageScheduler, "queueNameToNextRunTime", true));
  }

  @Test
  void destroyProducerMode() throws Exception {
    doReturn(true).when(rqueueConfig).isProducer();
    doReturn(true).when(rqueueSchedulerConfig).isEnabled();
    messageScheduler.onApplicationEvent(new RqueueBootstrapEvent("Test", true));
    messageScheduler.destroy();
  }

  @Test
  void destroyDisabled() throws Exception {
    doReturn(false).when(rqueueSchedulerConfig).isEnabled();
    messageScheduler.onApplicationEvent(new RqueueBootstrapEvent("Test", true));
    messageScheduler.destroy();
  }

  @Test
  void afterPropertiesSetDisabled() throws Exception {
    doReturn(false).when(rqueueSchedulerConfig).isEnabled();
    messageScheduler.onApplicationEvent(new RqueueBootstrapEvent("Test", true));
    assertNull(FieldUtils.readField(messageScheduler, "scheduler", true));
    assertNull(FieldUtils.readField(messageScheduler, "queueRunningState", true));
    assertNull(FieldUtils.readField(messageScheduler, "queueNameToScheduledTask", true));
    assertNull(FieldUtils.readField(messageScheduler, "queueNameToNextRunTime", true));
  }
}
