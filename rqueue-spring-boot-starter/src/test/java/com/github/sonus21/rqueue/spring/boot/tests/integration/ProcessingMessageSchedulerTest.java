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
import static com.github.sonus21.rqueue.utils.WaitForUtil.waitFor;
import static rqueue.test.Utility.buildMessage;

import com.github.sonus21.rqueue.core.RqueueMessage;
import com.github.sonus21.rqueue.exception.TimedOutException;
import com.github.sonus21.rqueue.producer.RqueueMessageSender;
import com.github.sonus21.rqueue.spring.boot.application.ApplicationWithCustomConfiguration;
import com.github.sonus21.rqueue.utils.QueueInfo;
import java.util.ArrayList;
import java.util.List;
import java.util.Map.Entry;
import java.util.Random;
import javax.annotation.PostConstruct;
import lombok.extern.slf4j.Slf4j;
import org.junit.Rule;
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
import rqueue.test.RunTestUntilFail;
import rqueue.test.Utility;
import rqueue.test.dto.Job;
import rqueue.test.service.ConsumedMessageService;

@SpringBootTest
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(classes = ApplicationWithCustomConfiguration.class)
@Slf4j
@TestPropertySource(
    properties = {
      "auto.start.scheduler=false",
      "spring.redis.port=6383",
      "mysql.db.name=test3",
      "max.workers.count=120"
    })
public class ProcessingMessageSchedulerTest {
  static {
    System.setProperty("TEST_NAME", ProcessingMessageSchedulerTest.class.getSimpleName());
  }

  @Autowired protected ConsumedMessageService consumedMessageService;
  @Autowired protected RqueueMessageSender messageSender;
  @Autowired protected RedisConnectionFactory redisConnectionFactory;
  private RedisTemplate<String, RqueueMessage> redisTemplate;

  @Value("${job.queue.name}")
  private String jobQueueName;
  @Rule
  public RunTestUntilFail retry =
      new RunTestUntilFail(
          log,
          1,
          () -> {
            for (Entry<String, List<RqueueMessage>> entry :
                Utility.getMessageMap(jobQueueName, redisTemplate).entrySet()) {
              log.error("FAILING Queue {}", entry.getKey());
              for (RqueueMessage message : entry.getValue()) {
                log.error("FAILING Queue {} Msg {}", entry.getKey(), message);
              }
            }
          });
  private int messageCount = 110;

  @PostConstruct
  public void init() {
    redisTemplate = getRedisTemplate(redisConnectionFactory);
  }

  @Test
  public void publishMessageIsTriggeredOnMessageRemoval()
      throws InterruptedException, TimedOutException {
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
      redisTemplate
          .opsForZSet()
          .add(
              processingQueueName,
              buildMessage(job, jobQueueName, null, null),
              currentTime + delay);
    }
    Thread.sleep(maxDelay);
    waitFor(
        () -> 0 == messageSender.getAllMessages(jobQueueName).size(),
        60 * 1000L,
        "messages to be consumed");
    waitFor(
        () -> messageCount == consumedMessageService.getMessages(ids, Job.class).size(),
        "message count to be matched");
    waitFor(
        () -> jobs.containsAll(consumedMessageService.getMessages(ids, Job.class).values()),
        "All jobs to be executed");
  }
}
