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
import static org.junit.Assert.assertTrue;

import com.github.sonus21.rqueue.listener.QueueDetail;
import com.github.sonus21.rqueue.utils.QueueUtils;
import org.junit.Test;
import org.mockito.Mock;
import org.springframework.data.redis.core.RedisTemplate;

public class ProcessingMessageSchedulerTest {
  private int poolSize = 1;
  @Mock private RedisTemplate<String, Long> redisTemplate;
  private ProcessingMessageScheduler messageScheduler =
      new ProcessingMessageScheduler(redisTemplate, poolSize, true, true, 900000);
  private String slowQueue = "slow-queue";
  private String fastQueue = "fast-queue";
  private QueueDetail slowQueueDetail = new QueueDetail(slowQueue, -1, "", true);
  private QueueDetail fastQueueDetail = new QueueDetail(fastQueue, -1, "", false);

  @Test
  public void getChannelName() {
    assertEquals(
        QueueUtils.getProcessingQueueChannelName(slowQueue),
        messageScheduler.getChannelName(slowQueue));
  }

  @Test
  public void getZsetName() {
    assertEquals(
        QueueUtils.getProcessingQueueName(slowQueue), messageScheduler.getZsetName(slowQueue));
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
        QueueUtils.getMessageReEnqueueTimeWithDelay(currentTime, 900000),
        messageScheduler.getNextScheduleTime(currentTime, null));
    assertEquals(
        currentTime + 1000L,
        messageScheduler.getNextScheduleTime(currentTime, currentTime + 1000L));
  }
}
