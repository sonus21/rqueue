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

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.doReturn;

import com.github.sonus21.rqueue.config.RqueueSchedulerConfig;
import com.github.sonus21.rqueue.core.DelayedMessageSchedulerTest.TestMessageScheduler;
import com.github.sonus21.rqueue.core.DelayedMessageSchedulerTest.TestThreadPoolScheduler;
import com.github.sonus21.rqueue.listener.QueueDetail;
import com.github.sonus21.rqueue.models.event.QueueInitializationEvent;
import com.github.sonus21.rqueue.utils.TestUtils;
import java.util.HashMap;
import java.util.Map;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.mockito.junit.MockitoJUnitRunner;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.listener.RedisMessageListenerContainer;

@RunWith(MockitoJUnitRunner.StrictStubs.class)
public class MessageSchedulerTest {
  @Mock private RqueueSchedulerConfig rqueueSchedulerConfig;
  @Mock private RedisMessageListenerContainer rqueueRedisMessageListenerContainer;
  @Mock private RedisTemplate<String, Long> redisTemplate;

  @InjectMocks private TestMessageScheduler messageScheduler;

  private String slowQueue = "slow-queue";
  private String fastQueue = "fast-queue";
  private QueueDetail slowQueueDetail =
      TestUtils.createQueueDetail(slowQueue, 3, true, 900000L, null);
  private QueueDetail fastQueueDetail =
      TestUtils.createQueueDetail(fastQueue, 3, false, 900000L, null);
  private Map<String, QueueDetail> queueNameToQueueDetail = new HashMap<>();

  @Before
  public void init() {
    MockitoAnnotations.initMocks(this);
    queueNameToQueueDetail.put(slowQueue, slowQueueDetail);
    queueNameToQueueDetail.put(fastQueue, fastQueueDetail);
  }

  @Test
  public void afterPropertiesSetWithEmptyQueSet() throws Exception {
    QueueRegistry.delete();
    messageScheduler.onApplicationEvent(new QueueInitializationEvent("Test", true));
    assertEquals(0, messageScheduler.scheduleList.size());
    messageScheduler.destroy();
  }

  @Test
  public void startShouldNotSubmitsTask() throws Exception {
    QueueRegistry.delete();
    QueueRegistry.register(slowQueueDetail);
    QueueRegistry.register(fastQueueDetail);
    doReturn(1).when(rqueueSchedulerConfig).getDelayedMessageThreadPoolSize();
    TestThreadPoolScheduler scheduler = new TestThreadPoolScheduler();
    FieldUtils.writeField(messageScheduler, "scheduler", scheduler, true);
    messageScheduler.onApplicationEvent(new QueueInitializationEvent("Test", true));
    assertEquals(0, scheduler.tasks.size());
    messageScheduler.destroy();
  }
}
