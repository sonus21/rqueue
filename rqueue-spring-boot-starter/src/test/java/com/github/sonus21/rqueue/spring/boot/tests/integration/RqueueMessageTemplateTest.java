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

package com.github.sonus21.rqueue.spring.boot.tests.integration;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.github.sonus21.rqueue.core.DefaultRqueueMessageConverter;
import com.github.sonus21.rqueue.core.RqueueMessage;
import com.github.sonus21.rqueue.core.support.RqueueMessageUtils;
import com.github.sonus21.rqueue.spring.boot.application.Application;
import com.github.sonus21.rqueue.spring.boot.tests.SpringBootIntegrationTest;
import com.github.sonus21.rqueue.test.common.SpringTestBase;
import com.github.sonus21.rqueue.test.dto.Email;
import com.github.sonus21.rqueue.test.dto.Job;
import com.github.sonus21.rqueue.test.dto.Notification;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.messaging.converter.MessageConverter;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestPropertySource;

@ContextConfiguration(classes = Application.class)
@SpringBootTest
@Slf4j
@TestPropertySource(properties = {"use.system.redis=false", "spring.data.redis.port:8004"})
@SpringBootIntegrationTest
class RqueueMessageTemplateTest extends SpringTestBase {

  @Test
  void moveMessageFromDeadLetterQueueToOriginalQueue() {
    enqueue(emailDeadLetterQueue, i -> Email.newInstance(), 10, true);
    assertTrue(
        rqueueMessageManager.moveMessageFromDeadLetterToQueue(emailDeadLetterQueue, emailQueue));
    assertEquals(10, stringRqueueRedisTemplate.getListSize(emailQueue).intValue());
    assertEquals(0, stringRqueueRedisTemplate.getListSize(emailDeadLetterQueue).intValue());
  }

  @Test
  void moveMessageFromOneQueueToAnother() {
    String queue1 = emailDeadLetterQueue + "t";
    String queue2 = emailQueue + "t";
    enqueue(queue1, i -> Email.newInstance(), 10, true);
    rqueueMessageTemplate.moveMessageListToList(queue1, queue2, 10);
    assertEquals(10, stringRqueueRedisTemplate.getListSize(queue2).intValue());
    assertEquals(0, stringRqueueRedisTemplate.getListSize(queue1).intValue());
    rqueueMessageTemplate.moveMessageListToList(queue2, queue1, 10);
    assertEquals(10, stringRqueueRedisTemplate.getListSize(queue1).intValue());
    assertEquals(0, stringRqueueRedisTemplate.getListSize(queue2).intValue());
  }

  @Test
  void moveMessageListToZset() {
    String queue = "moveMessageListToZset";
    String tgtZset = "moveMessageListToZsetTgt";
    enqueue(queue, i -> Email.newInstance(), 10, true);
    long score = System.currentTimeMillis() - 10000;
    rqueueMessageTemplate.moveMessageListToZset(queue, tgtZset, 10, score);
    assertEquals(0, stringRqueueRedisTemplate.getListSize(queue).intValue());
    assertEquals(10, stringRqueueRedisTemplate.getZsetSize(tgtZset).intValue());
  }

  @Test
  void moveMessageZsetToList() {
    String zset = notificationQueue + "zset";
    String list = notificationQueue + "list";
    enqueueIn(zset, i -> Notification.newInstance(), i -> 50000L, 10, true);
    rqueueMessageTemplate.moveMessageZsetToList(zset, list, 10);
    assertEquals(10, stringRqueueRedisTemplate.getListSize(list).intValue());
    assertEquals(0, stringRqueueRedisTemplate.getZsetSize(zset).intValue());
  }

  @Test
  void moveMessageZsetToZset() {
    String srcZset = jobQueue + "src";
    String tgtZset = jobQueue + "tgt";
    enqueueIn(srcZset, i -> Job.newInstance(), i -> 5000L, 10, true);
    rqueueMessageTemplate.moveMessageZsetToZset(srcZset, tgtZset, 10, 0, false);
    assertEquals(10, stringRqueueRedisTemplate.getZsetSize(tgtZset).intValue());
    assertEquals(0, stringRqueueRedisTemplate.getZsetSize(srcZset).intValue());

    rqueueMessageTemplate.moveMessageZsetToZset(tgtZset, "_rq::xx" + jobQueue, 10, 0, false);
    assertEquals(0, stringRqueueRedisTemplate.getZsetSize(tgtZset).intValue());
    assertEquals(10, stringRqueueRedisTemplate.getZsetSize("_rq::xx" + jobQueue).intValue());
  }

  @Test
  void getScore() {
    String tgtZset = "getScoreZSet";
    MessageConverter converter = new DefaultRqueueMessageConverter();
    RqueueMessage message = RqueueMessageUtils.generateMessage(converter, tgtZset);
    RqueueMessage message2 = RqueueMessageUtils.generateMessage(converter, tgtZset);
    RqueueMessage message3 = RqueueMessageUtils.generateMessage(converter, tgtZset);
    RqueueMessage message4 = RqueueMessageUtils.generateMessage(converter, tgtZset);
    RqueueMessage message5 = RqueueMessageUtils.generateMessage(converter, tgtZset);
    long score = System.currentTimeMillis();
    rqueueMessageTemplate.addToZset(tgtZset, message2, score);
    rqueueMessageTemplate.addToZset(tgtZset, message3, 0);
    rqueueMessageTemplate.addToZset(tgtZset, message4, -1000);
    rqueueMessageTemplate.addToZset(tgtZset, message5, -1);
    assertNull(rqueueMessageTemplate.getScore(tgtZset, message));
    assertEquals(score, rqueueMessageTemplate.getScore(tgtZset, message2));
    assertEquals(0, rqueueMessageTemplate.getScore(tgtZset, message3));
    assertEquals(-1000, rqueueMessageTemplate.getScore(tgtZset, message4));
    assertEquals(-1, rqueueMessageTemplate.getScore(tgtZset, message5));
  }

  @Test
  void updateScore() {
    String tgtZset = "updateScoreZSet";
    MessageConverter converter = new DefaultRqueueMessageConverter();
    RqueueMessage message = RqueueMessageUtils.generateMessage(converter, tgtZset);
    RqueueMessage message2 = RqueueMessageUtils.generateMessage(converter, tgtZset);
    RqueueMessage message3 = RqueueMessageUtils.generateMessage(converter, tgtZset);
    long score = System.currentTimeMillis();
    rqueueMessageTemplate.addToZset(tgtZset, message, score);
    assertTrue(rqueueMessageTemplate.addScore(tgtZset, message, 10_000));
    assertEquals(score + 10_000, rqueueMessageTemplate.getScore(tgtZset, message));

    rqueueMessageTemplate.addToZset(tgtZset, message3, 0);
    assertTrue(rqueueMessageTemplate.addScore(tgtZset, message3, 10_000));
    assertEquals(10_000, rqueueMessageTemplate.getScore(tgtZset, message3));

    assertFalse(rqueueMessageTemplate.addScore(tgtZset, message2, 10_000));
  }
}
