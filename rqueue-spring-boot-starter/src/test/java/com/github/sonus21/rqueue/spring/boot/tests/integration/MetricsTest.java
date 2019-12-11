/*
 * Copyright 2019 Sonu Kumar
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

import static com.github.sonus21.rqueue.utils.RedisUtil.getRedisTemplate;
import static com.github.sonus21.rqueue.utils.TimeUtil.waitFor;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static rqueue.test.Utility.buildMessage;

import com.github.sonus21.rqueue.core.RqueueMessage;
import com.github.sonus21.rqueue.exception.TimedOutException;
import com.github.sonus21.rqueue.producer.RqueueMessageSender;
import com.github.sonus21.rqueue.spring.boot.application.Application;
import com.github.sonus21.rqueue.utils.QueueInfo;
import io.micrometer.core.instrument.MeterRegistry;
import java.util.Random;
import javax.annotation.PostConstruct;
import lombok.extern.slf4j.Slf4j;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import rqueue.test.dto.Email;
import rqueue.test.dto.Job;
import rqueue.test.dto.Notification;
import rqueue.test.service.ConsumedMessageService;
import rqueue.test.service.FailureManager;

@SpringBootTest
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(classes = Application.class)
@Slf4j
@TestPropertySource(
    properties = {
      "auto.start.scheduler=false",
      "spring.redis.port=6384",
      "mysql.db.name=test4",
      "rqueue.metrics.count.failure=true",
      "rqueue.metrics.count.execution=true",
    })
public class MetricsTest {
  static {
    System.setProperty("TEST_NAME", MetricsTest.class.getSimpleName());
  }

  @Autowired protected ConsumedMessageService consumedMessageService;
  @Autowired protected RqueueMessageSender messageSender;
  @Autowired protected FailureManager failureManager;
  @Autowired protected RedisConnectionFactory redisConnectionFactory;
  @Autowired private MeterRegistry meterRegistry;
  private RedisTemplate<String, RqueueMessage> redisTemplate;

  @Value("${email.dead.letter.queue.name}")
  private String emailDlq;

  @Value("${email.queue.name}")
  private String emailQueueName;

  @Value("${job.queue.name}")
  private String jobQueue;

  @Value("${notification.queue.name}")
  private String notificationQueue;

  @PostConstruct
  public void init() {
    redisTemplate = getRedisTemplate(redisConnectionFactory);
  }

  @Test
  public void delayedQueueStatus() throws TimedOutException {
    Random random = new Random();
    long maxDelay = 0;
    int maxMessages = 100;
    for (int i = 0; i < maxMessages; i++) {
      long delay = random.nextInt(10000);
      if (maxDelay < delay) {
        maxDelay = delay;
      }
      Notification notification = Notification.newInstance();
      if (i < maxMessages / 2) {
        redisTemplate
            .opsForZSet()
            .add(
                QueueInfo.getTimeQueueName(notificationQueue),
                buildMessage(notification, notificationQueue, null, null),
                System.currentTimeMillis() - delay);
      } else {
        messageSender.put(notificationQueue, notification, delay);
      }
    }

    if (maxDelay == 5000) {
      Notification notification = Notification.newInstance();
      messageSender.put(notificationQueue, notification, 10000);
    }

    assertTrue(
        meterRegistry
                .get("delayed.queue.size")
                .tag("rqueue", "test")
                .tag("queue", notificationQueue)
                .gauge()
                .value()
            > 0);
    waitFor(
        () ->
            meterRegistry
                    .get("queue.size")
                    .tag("rqueue", "test")
                    .tag("queue", notificationQueue)
                    .gauge()
                    .value()
                > 0,
        "Message in original queue");
  }

  @Test
  public void metricStatus() throws TimedOutException {
    for (int i = 0; i < 10; i++) {
      Email email = Email.newInstance();
      redisTemplate
          .opsForList()
          .leftPush(emailDlq, buildMessage(email, emailQueueName, null, null));
    }

    Job job = Job.newInstance();
    failureManager.createFailureDetail(job.getId(), -1, 0);
    messageSender.put(jobQueue, job);

    assertEquals(
        10,
        meterRegistry
            .get("dead.letter.queue.size")
            .tags("rqueue", "test")
            .tags("queue", emailQueueName)
            .gauge()
            .value(),
        0);
    waitFor(
        () ->
            meterRegistry
                    .get("processing.queue.size")
                    .tags("rqueue", "test")
                    .tags("queue", jobQueue)
                    .gauge()
                    .value()
                == 1,
        "processing queue message");
  }

  @Test
  public void countStatus() throws TimedOutException {
    messageSender.put(emailQueueName, Email.newInstance());
    Job job = Job.newInstance();
    failureManager.createFailureDetail(job.getId(), 1, 1);
    messageSender.put(jobQueue, job);
    waitFor(
        () ->
            meterRegistry
                    .get("execution.count")
                    .tags("rqueue", "test")
                    .tags("queue", emailQueueName)
                    .counter()
                    .count()
                == 1,
        "message process");

    assertEquals(
        0,
        meterRegistry
            .get("failure.count")
            .tags("rqueue", "test")
            .tags("queue", emailQueueName)
            .counter()
            .count(),
        0);
    waitFor(
        () ->
            meterRegistry
                    .get("failure.count")
                    .tags("rqueue", "test")
                    .tags("queue", jobQueue)
                    .counter()
                    .count()
                >= 1,
        "job process");
  }
}
