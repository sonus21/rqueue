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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.doReturn;

import com.github.sonus21.TestBase;
import com.github.sonus21.rqueue.CoreUnitTest;
import com.github.sonus21.rqueue.config.RqueueSchedulerConfig;
import com.github.sonus21.rqueue.listener.QueueDetail;
import com.github.sonus21.rqueue.utils.TestUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.listener.RedisMessageListenerContainer;

@CoreUnitTest
class ProcessingQueueMessageSchedulerTest extends TestBase {

  private final String slowQueue = "slow-queue";
  private final String fastQueue = "fast-queue";
  private final QueueDetail slowQueueDetail = TestUtils.createQueueDetail(slowQueue);
  private final QueueDetail fastQueueDetail = TestUtils.createQueueDetail(fastQueue);
  @Mock private RedisTemplate<String, Long> redisTemplate;
  @Mock private RqueueSchedulerConfig rqueueSchedulerConfig;
  @Mock private RedisMessageListenerContainer redisMessageListenerContainer;
  @InjectMocks private ProcessingQueueMessageScheduler messageScheduler;

  @BeforeEach
  public void init() {
    MockitoAnnotations.openMocks(this);
    EndpointRegistry.delete();
    EndpointRegistry.register(slowQueueDetail);
    EndpointRegistry.register(fastQueueDetail);
    doReturn(1).when(rqueueSchedulerConfig).getProcessingMessageThreadPoolSize();
    messageScheduler.initialize();
  }

  @Test
  void getChannelName() {
    assertEquals(
        slowQueueDetail.getProcessingQueueChannelName(),
        messageScheduler.getChannelName(slowQueue));
  }

  @Test
  void getZsetName() {
    assertEquals(slowQueueDetail.getProcessingQueueName(), messageScheduler.getZsetName(slowQueue));
  }

  @Test
  void getNextScheduleTimeSlowQueue() {
    long currentTime = System.currentTimeMillis();
    assertThat(
        messageScheduler.getNextScheduleTime(slowQueue, null),
        greaterThanOrEqualTo(currentTime + 100000));
    assertEquals(
        currentTime + 1000L, messageScheduler.getNextScheduleTime(slowQueue, currentTime + 1000L));
  }

  @Test
  void getNextScheduleTimeFastQueue() {
    long currentTime = System.currentTimeMillis();
    assertThat(
        messageScheduler.getNextScheduleTime(fastQueue, null),
        greaterThanOrEqualTo(currentTime + 200000));
    assertEquals(
        currentTime + 1000L, messageScheduler.getNextScheduleTime(fastQueue, currentTime + 1000L));
  }
}
