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
import com.github.sonus21.rqueue.spring.boot.application.ApplicationListenerDisabled;
import com.github.sonus21.rqueue.utils.QueueInfo;
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
import rqueue.test.service.ConsumedMessageService;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(classes = ApplicationListenerDisabled.class)
@Slf4j
@TestPropertySource(
    properties = {"auto.start.scheduler=false", "spring.redis.port=6382", "mysql.db.name=test2"})
@SpringBootTest
public class MessageChannelTest {
  static {
    System.setProperty("TEST_NAME", MessageChannelTest.class.getSimpleName());
  }

  @Autowired private ConsumedMessageService consumedMessageService;
  @Autowired private RqueueMessageSender messageSender;
  @Autowired private RedisConnectionFactory redisConnectionFactory;
  private RedisTemplate<String, RqueueMessage> redisTemplate;
  @Value("${email.queue.name}")
  private String emailQueue;

  @PostConstruct
  public void init() {
    redisTemplate = getRedisTemplate(redisConnectionFactory);
  }

  @Test
  public void publishMessageIsTriggeredOnMessageAddition() throws TimedOutException {
    long currentTime = System.currentTimeMillis();
    Email email;
    int messageCount = 200;
    for (int i = 0; i < messageCount; i++) {
      email = Email.newInstance();
      redisTemplate
          .opsForZSet()
          .add(
              QueueInfo.getTimeQueueName(emailQueue),
              buildMessage(email, emailQueue, null, null),
              currentTime - 1000L);
    }
    email = Email.newInstance();
    log.info("adding new message {}", email);
    messageSender.put(emailQueue, email, 1000L);
    waitFor(
        () -> redisTemplate.opsForZSet().size(QueueInfo.getTimeQueueName(emailQueue)) <= 1,
        "one or less messages in zset");
    assertTrue(
        "Messages are correctly moved",
        redisTemplate.opsForList().size(emailQueue) >= messageCount);
    assertEquals(messageCount + 1, messageSender.getAllMessages(emailQueue).size());
  }
}
