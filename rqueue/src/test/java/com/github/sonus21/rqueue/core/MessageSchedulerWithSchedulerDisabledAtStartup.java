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

import com.github.sonus21.rqueue.core.DelayedMessageSchedulerTest.TestMessageScheduler;
import com.github.sonus21.rqueue.core.DelayedMessageSchedulerTest.TestTaskScheduler;
import com.github.sonus21.rqueue.event.QueueInitializationEvent;
import com.github.sonus21.rqueue.listener.QueueDetail;
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
public class MessageSchedulerWithSchedulerDisabledAtStartup {
  @Mock private RedisMessageListenerContainer redisMessageListenerContainer;
  @Mock private RedisTemplate<String, Long> redisTemplate;

  @InjectMocks
  private TestMessageScheduler messageScheduler =
      new TestMessageScheduler(redisTemplate, 1, false, true);

  private String slowQueue = "slow-queue";
  private String fastQueue = "fast-queue";
  private QueueDetail slowQueueDetail = new QueueDetail(slowQueue, -1, "", true, 900000L);
  private QueueDetail fastQueueDetail = new QueueDetail(fastQueue, -1, "", false, 900000L);
  private Map<String, QueueDetail> queueNameToQueueDetail = new HashMap<>();

  @Before
  public void init() {
    MockitoAnnotations.initMocks(this);
    queueNameToQueueDetail.put(slowQueue, slowQueueDetail);
    queueNameToQueueDetail.put(fastQueue, fastQueueDetail);
  }

  @Test
  public void afterPropertiesSetWithEmptyQueSet() throws Exception {
    messageScheduler.onApplicationEvent(new QueueInitializationEvent("Test", null, true));
    assertEquals(0, messageScheduler.scheduleList.size());
    messageScheduler.destroy();
  }

  @Test
  public void startShouldNotSubmitsTask() throws Exception {
    TestTaskScheduler scheduler = new TestTaskScheduler();
    FieldUtils.writeField(messageScheduler, "scheduler", scheduler, true);
    messageScheduler.onApplicationEvent(
        new QueueInitializationEvent("Test", queueNameToQueueDetail, true));
    assertEquals(0, scheduler.tasks.size());
    messageScheduler.destroy();
  }
}
