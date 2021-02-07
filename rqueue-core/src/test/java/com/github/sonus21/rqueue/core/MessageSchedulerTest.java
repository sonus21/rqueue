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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.doReturn;

import com.github.sonus21.TestBase;
import com.github.sonus21.rqueue.CoreUnitTest;
import com.github.sonus21.rqueue.config.RqueueSchedulerConfig;
import com.github.sonus21.rqueue.core.DelayedMessageSchedulerTest.TestMessageScheduler;
import com.github.sonus21.rqueue.core.DelayedMessageSchedulerTest.TestThreadPoolScheduler;
import com.github.sonus21.rqueue.listener.QueueDetail;
import com.github.sonus21.rqueue.models.event.RqueueBootstrapEvent;
import com.github.sonus21.rqueue.utils.TestUtils;
import java.util.HashMap;
import java.util.Map;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.listener.RedisMessageListenerContainer;

@CoreUnitTest
class MessageSchedulerTest extends TestBase {

  @Mock
  private RqueueSchedulerConfig rqueueSchedulerConfig;
  @Mock
  private RedisMessageListenerContainer rqueueRedisMessageListenerContainer;
  @Mock
  private RedisTemplate<String, Long> redisTemplate;

  @InjectMocks
  private TestMessageScheduler messageScheduler;

  private final String slowQueue = "slow-queue";
  private final String fastQueue = "fast-queue";
  private final QueueDetail slowQueueDetail = TestUtils.createQueueDetail(slowQueue);
  private final QueueDetail fastQueueDetail = TestUtils.createQueueDetail(fastQueue);
  private final Map<String, QueueDetail> queueNameToQueueDetail = new HashMap<>();

  @BeforeEach
  public void init() {
    MockitoAnnotations.openMocks(this);
    queueNameToQueueDetail.put(slowQueue, slowQueueDetail);
    queueNameToQueueDetail.put(fastQueue, fastQueueDetail);
  }

  @Test
  void afterPropertiesSetWithEmptyQueSet() throws Exception {
    EndpointRegistry.delete();
    messageScheduler.onApplicationEvent(new RqueueBootstrapEvent("Test", true));
    assertEquals(0, messageScheduler.scheduleList.size());
    messageScheduler.destroy();
  }

  @Test
  void startShouldNotSubmitsTask() throws Exception {
    EndpointRegistry.delete();
    EndpointRegistry.register(slowQueueDetail);
    EndpointRegistry.register(fastQueueDetail);
    doReturn(1).when(rqueueSchedulerConfig).getDelayedMessageThreadPoolSize();
    TestThreadPoolScheduler scheduler = new TestThreadPoolScheduler();
    FieldUtils.writeField(messageScheduler, "scheduler", scheduler, true);
    messageScheduler.onApplicationEvent(new RqueueBootstrapEvent("Test", true));
    assertEquals(0, scheduler.tasks.size());
    messageScheduler.destroy();
  }
}
