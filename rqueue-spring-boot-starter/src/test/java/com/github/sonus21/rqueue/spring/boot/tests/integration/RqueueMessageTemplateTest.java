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

package com.github.sonus21.rqueue.spring.boot.tests.integration;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.github.sonus21.rqueue.spring.boot.application.Application;
import com.github.sonus21.rqueue.test.common.SpringTestBase;
import com.github.sonus21.rqueue.test.dto.Email;
import com.github.sonus21.rqueue.test.dto.Job;
import com.github.sonus21.rqueue.test.dto.Notification;
import com.github.sonus21.test.RqueueSpringTestRunner;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestPropertySource;

@ExtendWith(RqueueSpringTestRunner.class)
@ContextConfiguration(classes = Application.class)
@SpringBootTest
@Slf4j
@TestPropertySource(properties = {"use.system.redis=false", "spring.redis.port:8004"})
public class RqueueMessageTemplateTest extends SpringTestBase {
  @Test
  public void moveMessageFromDeadLetterQueueToOriginalQueue() {
    enqueue(emailDeadLetterQueue, i -> Email.newInstance(), 10);
    rqueueMessageSender.moveMessageFromDeadLetterToQueue(emailDeadLetterQueue, emailQueue);
    assertEquals(10, stringRqueueRedisTemplate.getListSize(emailQueue).intValue());
    assertEquals(0, stringRqueueRedisTemplate.getListSize(emailDeadLetterQueue).intValue());
  }

  @Test
  public void moveMessageFromOneQueueToAnother() {
    String queue1 = emailDeadLetterQueue + "t";
    String queue2 = emailQueue + "t";
    enqueue(queue1, i -> Email.newInstance(), 10);
    rqueueMessageTemplate.moveMessageListToList(queue1, queue2, 10);
    assertEquals(10, stringRqueueRedisTemplate.getListSize(queue2).intValue());
    assertEquals(0, stringRqueueRedisTemplate.getListSize(queue1).intValue());
    rqueueMessageTemplate.moveMessageListToList(queue2, queue1, 10);
    assertEquals(10, stringRqueueRedisTemplate.getListSize(queue1).intValue());
    assertEquals(0, stringRqueueRedisTemplate.getListSize(queue2).intValue());
  }

  @Test
  public void moveMessageListToZset() {
    String queue = "moveMessageListToZset";
    String tgtZset = "moveMessageListToZsetTgt";
    enqueue(queue, i -> Email.newInstance(), 10);
    long score = System.currentTimeMillis() - 10000;
    rqueueMessageTemplate.moveMessageListToZset(queue, tgtZset, 10, score);
    assertEquals(0, stringRqueueRedisTemplate.getListSize(queue).intValue());
    assertEquals(10, stringRqueueRedisTemplate.getZsetSize(tgtZset).intValue());
  }

  @Test
  public void moveMessageZsetToList() {
    String zset = notificationQueue + "zset";
    String list = notificationQueue + "list";
    enqueueIn(zset, i -> Notification.newInstance(), i -> 50000L, 10);
    rqueueMessageTemplate.moveMessageZsetToList(zset, list, 10);
    assertEquals(10, stringRqueueRedisTemplate.getListSize(list).intValue());
    assertEquals(0, stringRqueueRedisTemplate.getZsetSize(zset).intValue());
  }

  @Test
  public void moveMessageZsetToZset() {
    String srcZset = jobQueue + "src";
    String tgtZset = jobQueue + "tgt";
    enqueueIn(srcZset, i -> Job.newInstance(), i -> 5000L, 10);
    rqueueMessageTemplate.moveMessageZsetToZset(srcZset, tgtZset, 10, 0, false);
    assertEquals(10, stringRqueueRedisTemplate.getZsetSize(tgtZset).intValue());
    assertEquals(0, stringRqueueRedisTemplate.getZsetSize(srcZset).intValue());

    rqueueMessageTemplate.moveMessageZsetToZset(tgtZset, "_rq::xx" + jobQueue, 10, 0, false);
    assertEquals(0, stringRqueueRedisTemplate.getZsetSize(tgtZset).intValue());
    assertEquals(10, stringRqueueRedisTemplate.getZsetSize("_rq::xx" + jobQueue).intValue());
  }
}
