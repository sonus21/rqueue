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

package com.github.sonus21.rqueue.spring.boot.integration;

import static com.github.sonus21.rqueue.utils.RedisUtil.getRedisTemplate;
import static com.github.sonus21.rqueue.utils.TimeUtil.waitFor;

import com.github.sonus21.rqueue.converter.GenericMessageConverter;
import com.github.sonus21.rqueue.core.RqueueMessage;
import com.github.sonus21.rqueue.exception.TimedOutException;
import com.github.sonus21.rqueue.producer.RqueueMessageSender;
import com.github.sonus21.rqueue.spring.boot.integration.app.dto.EmailTask;
import com.github.sonus21.rqueue.spring.boot.integration.app.dto.Job;
import com.github.sonus21.rqueue.spring.boot.integration.app.service.ConsumedMessageService;
import com.github.sonus21.rqueue.spring.boot.integration.app.service.FailureManager;
import com.github.sonus21.rqueue.utils.QueueInfo;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.messaging.Message;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(classes = ApplicationWithAutoStartSchedulerDisabled.class)
@Ignore
public class ApplicationWithAutoStartSchedulerDisabledTest {
  @Autowired private ConsumedMessageService consumedMessageService;
  @Autowired private RqueueMessageSender messageSender;
  @Autowired private RedisConnectionFactory redisConnectionFactory;
  @Autowired private FailureManager failureManager;

  @Value("${email.queue.name}")
  private String emailQueue;

  @Value("${job.queue.name}")
  private String jobQueueName;

  private int messageCount = 10;

  @Test
  public void testPublishMessageIsTriggeredOnMessageAddition()
      throws InterruptedException, TimedOutException {
    List<EmailTask> emailTasks = new ArrayList<>();
    List<String> ids = new ArrayList<>();
    for (int i = 0; i < messageCount; i++) {
      EmailTask emailTask = EmailTask.newInstance();
      messageSender.put(emailQueue, emailTask, 1000L);
      emailTasks.add(emailTask);
      ids.add(emailTask.getId());
    }
    Thread.sleep(5000);
    messageSender.put(emailQueue, EmailTask.newInstance());
    waitFor(
        () -> 1 == messageSender.getAllMessages(emailQueue).size(),
        60000L,
        "messages to be consumed");
    waitFor(
        () -> messageCount == consumedMessageService.getMessages(ids, EmailTask.class).size(),
        "message count to be matched");
    waitFor(
        () ->
            emailTasks.containsAll(
                consumedMessageService.getMessages(ids, EmailTask.class).values()),
        "All messages to be consumed");
  }

  private RqueueMessage buildMessage(Object object) {
    GenericMessageConverter converter = new GenericMessageConverter();
    Message<?> msg = converter.toMessage(object, null);
    return new RqueueMessage(jobQueueName, (String) msg.getPayload(), 1, 100L);
  }

  @Test
  public void testPublishMessageIsTriggeredOnMessageRemoval()
      throws InterruptedException, TimedOutException {
    RedisTemplate<String, RqueueMessage> redisTemplate = getRedisTemplate(redisConnectionFactory);
    String processingQueueName = QueueInfo.getProcessingQueueName(jobQueueName);
    long currentTime = System.currentTimeMillis();
    List<Job> jobs = new ArrayList<>();
    List<String> ids = new ArrayList<>();
    int maxDelay = 2000;
    Random random = new Random();
    for (int i = 0; i < messageCount; i++) {
      Job job = Job.newInstance();
      jobs.add(job);
      ids.add(job.getId());
      int delay = random.nextInt(maxDelay);
      if (random.nextBoolean()) {
        delay = delay * -1;
      }
      redisTemplate.opsForZSet().add(processingQueueName, buildMessage(job), currentTime + delay);
    }
    Thread.sleep(maxDelay);
    waitFor(
        () -> 0 == messageSender.getAllMessages(jobQueueName).size(),
        30000L,
        "messages to be consumed");
    waitFor(
        () -> messageCount == consumedMessageService.getMessages(ids, Job.class).size(),
        "message count to be matched");
    waitFor(
        () -> jobs.containsAll(consumedMessageService.getMessages(ids, Job.class).values()),
        "All jobs to be executed");
  }
}
