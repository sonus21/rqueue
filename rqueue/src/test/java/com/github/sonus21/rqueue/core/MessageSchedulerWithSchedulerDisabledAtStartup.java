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

package com.github.sonus21.rqueue.core;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.doReturn;

import com.github.sonus21.rqueue.core.MessageSchedulerTest.TestMessageScheduler;
import com.github.sonus21.rqueue.core.MessageSchedulerTest.TestTaskScheduler;
import com.github.sonus21.rqueue.listener.ConsumerQueueDetail;
import com.github.sonus21.rqueue.listener.RqueueMessageListenerContainer;
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
@SuppressWarnings({"ResultOfMethodCallIgnored", "unused"})
public class MessageSchedulerWithSchedulerDisabledAtStartup {
  @Mock private RqueueMessageListenerContainer rqueueMessageListenerContainer;
  @Mock private RedisMessageListenerContainer redisMessageListenerContainer;
  @Mock private RedisTemplate<String, Long> redisTemplate;

  @InjectMocks
  private TestMessageScheduler messageScheduler = new TestMessageScheduler(redisTemplate, 1, false);

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
  public void afterPropertiesSetWithEmptyQueSet() throws Exception {
    doReturn(queueNameToQueueDetail).when(rqueueMessageListenerContainer).getRegisteredQueues();
    messageScheduler.afterPropertiesSet();
    messageScheduler.start();
    assertEquals(0, messageScheduler.scheduleList.size());
    messageScheduler.destroy();
  }

  @Test
  public void startShouldNotSubmitsTask() throws Exception {
    doReturn(queueNameToQueueDetail).when(rqueueMessageListenerContainer).getRegisteredQueues();
    messageScheduler.afterPropertiesSet();
    TestTaskScheduler scheduler = new TestTaskScheduler();
    FieldUtils.writeField(messageScheduler, "scheduler", scheduler, true);
    messageScheduler.start();
    assertEquals(0, scheduler.tasks.size());
    messageScheduler.destroy();
  }
}
