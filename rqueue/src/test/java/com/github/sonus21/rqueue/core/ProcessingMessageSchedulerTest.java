/*
 * Copyright (c) 2019-2019, Sonu Kumar
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
import static org.junit.Assert.assertTrue;

import com.github.sonus21.rqueue.listener.ConsumerQueueDetail;
import com.github.sonus21.rqueue.utils.QueueInfo;
import org.junit.Test;
import org.mockito.Mock;
import org.springframework.data.redis.core.RedisTemplate;

public class ProcessingMessageSchedulerTest {
  private int poolSize = 1;
  @Mock private RedisTemplate<String, Long> redisTemplate;
  private ProcessingMessageScheduler messageScheduler =
      new ProcessingMessageScheduler(redisTemplate, poolSize, true);
  private String slowQueue = "slow-queue";
  private String fastQueue = "fast-queue";
  private ConsumerQueueDetail slowQueueDetail = new ConsumerQueueDetail(slowQueue, -1, "", true);
  private ConsumerQueueDetail fastQueueDetail = new ConsumerQueueDetail(fastQueue, -1, "", false);

  @Test
  public void getChannelName() {
    assertEquals(
        QueueInfo.getProcessingQueueChannelName(slowQueue),
        messageScheduler.getChannelName(slowQueue));
  }

  @Test
  public void getZsetName() {
    assertEquals(
        QueueInfo.getProcessingQueueName(slowQueue), messageScheduler.getZsetName(slowQueue));
  }

  @Test
  public void isQueueValid() {
    assertTrue(messageScheduler.isQueueValid(slowQueueDetail));
    assertTrue(messageScheduler.isQueueValid(fastQueueDetail));
  }

  @Test
  public void getNextScheduleTime() {
    long currentTime = System.currentTimeMillis();
    assertEquals(
        QueueInfo.getMessageReEnqueueTime(currentTime),
        messageScheduler.getNextScheduleTime(currentTime, null));
    assertEquals(
        currentTime + 1000L,
        messageScheduler.getNextScheduleTime(currentTime, currentTime + 1000L));
  }
}
