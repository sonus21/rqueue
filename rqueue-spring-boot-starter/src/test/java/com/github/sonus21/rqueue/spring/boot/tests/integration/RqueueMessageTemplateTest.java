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

import com.github.sonus21.rqueue.common.RqueueRedisTemplate;
import com.github.sonus21.rqueue.core.RqueueMessageTemplate;
import com.github.sonus21.rqueue.exception.TimedOutException;
import com.github.sonus21.rqueue.producer.RqueueMessageSender;
import com.github.sonus21.rqueue.spring.boot.application.Application;
import com.github.sonus21.rqueue.test.dto.Email;
import com.github.sonus21.rqueue.test.dto.Job;
import com.github.sonus21.rqueue.test.dto.Notification;
import com.github.sonus21.rqueue.utils.QueueUtils;
import com.github.sonus21.test.RqueueSpringTestRunner;
import lombok.extern.slf4j.Slf4j;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestPropertySource;

@RunWith(RqueueSpringTestRunner.class)
@ContextConfiguration(classes = Application.class)
@SpringBootTest
@Slf4j
@TestPropertySource(properties = {"use.system.redis=false", "spring.redis.port:6385"})
public class RqueueMessageTemplateTest {
  @Autowired private RqueueMessageTemplate rqueueMessageTemplate;
  @Autowired private RqueueMessageSender messageSender;
  @Autowired private RqueueRedisTemplate<String> stringRqueueRedisTemplate;

  @Value("${job.queue.name}")
  private String jobQueueName;

  @Value("${email.queue.name}")
  private String emailQueue;

  @Value("${email.dead.letter.queue.name}")
  private String emailDeadLetterQueue;

  @Value("${email.queue.retry.count}")
  private int emailRetryCount;

  @Value("${notification.queue.name}")
  private String notificationQueue;

  @Value("${notification.queue.retry.count}")
  private int notificationRetryCount;

  @Test
  public void moveMessageFromDeadLetterQueueToOriginalQueue() throws TimedOutException {
    for (int i = 0; i < 10; i++) {
      Email email = Email.newInstance();
      messageSender.enqueue(emailDeadLetterQueue, email);
    }
    messageSender.moveMessageFromDeadLetterToQueue(emailDeadLetterQueue, emailQueue);
    Assert.assertEquals(10, stringRqueueRedisTemplate.getListSize(emailQueue).intValue());
    Assert.assertEquals(0, stringRqueueRedisTemplate.getListSize(emailDeadLetterQueue).intValue());
  }

  @Test
  public void moveMessageFromOneQueueToAnother() throws TimedOutException {
    String queue1 = emailDeadLetterQueue + "t";
    String queue2 = emailQueue + "t";
    for (int i = 0; i < 10; i++) {
      Email email = Email.newInstance();
      messageSender.enqueue(queue1, email);
    }
    rqueueMessageTemplate.moveMessageListToList(queue1, queue2, 10);
    Assert.assertEquals(10, stringRqueueRedisTemplate.getListSize(queue2).intValue());
    Assert.assertEquals(0, stringRqueueRedisTemplate.getListSize(queue1).intValue());
    rqueueMessageTemplate.moveMessageListToList(queue2, queue1, 10);
    Assert.assertEquals(10, stringRqueueRedisTemplate.getListSize(queue1).intValue());
    Assert.assertEquals(0, stringRqueueRedisTemplate.getListSize(queue2).intValue());
  }

  @Test
  public void moveMessageListToZset() throws TimedOutException {
    stringRqueueRedisTemplate.delete(notificationQueue);
    stringRqueueRedisTemplate.delete(QueueUtils.getDelayedQueueName(notificationQueue));
    for (int i = 0; i < 10; i++) {
      Notification notification = Notification.newInstance();
      messageSender.enqueue(notificationQueue, notification);
    }
    long score = System.currentTimeMillis() - 10000;
    rqueueMessageTemplate.moveMessageListToZset(
        notificationQueue, QueueUtils.getDelayedQueueName(notificationQueue), 10, score);

    Assert.assertEquals(0, stringRqueueRedisTemplate.getListSize(notificationQueue).intValue());
    Assert.assertEquals(
        10,
        stringRqueueRedisTemplate
            .getZsetSize(QueueUtils.getDelayedQueueName(notificationQueue))
            .intValue());
  }

  @Test
  public void moveMessageZsetToList() throws TimedOutException {
    String queue = notificationQueue + "t";
    for (int i = 0; i < 10; i++) {
      Notification notification = Notification.newInstance();
      messageSender.enqueueIn(queue, notification, 50000L);
    }
    rqueueMessageTemplate.moveMessageZsetToList(QueueUtils.getDelayedQueueName(queue), queue, 10);
    Assert.assertEquals(10, stringRqueueRedisTemplate.getListSize(queue).intValue());
    Assert.assertEquals(
        0, stringRqueueRedisTemplate.getZsetSize(QueueUtils.getDelayedQueueName(queue)).intValue());
  }

  @Test
  public void moveMessageZsetToZset() throws TimedOutException {
    String tgtZset = jobQueueName + "t";
    for (int i = 0; i < 10; i++) {
      Job job = Job.newInstance();
      messageSender.enqueueIn(jobQueueName, job, 50000L);
    }
    rqueueMessageTemplate.moveMessageZsetToZset(
        QueueUtils.getDelayedQueueName(jobQueueName), tgtZset, 10, 0, false);
    Assert.assertEquals(10, stringRqueueRedisTemplate.getZsetSize(tgtZset).intValue());
    Assert.assertEquals(0, stringRqueueRedisTemplate.getZsetSize(jobQueueName).intValue());

    rqueueMessageTemplate.moveMessageZsetToZset(
        tgtZset, QueueUtils.getDelayedQueueName(jobQueueName), 10, 0, false);
    Assert.assertEquals(0, stringRqueueRedisTemplate.getZsetSize(tgtZset).intValue());
    Assert.assertEquals(
        10,
        stringRqueueRedisTemplate
            .getZsetSize(QueueUtils.getDelayedQueueName(jobQueueName))
            .intValue());
  }
}
