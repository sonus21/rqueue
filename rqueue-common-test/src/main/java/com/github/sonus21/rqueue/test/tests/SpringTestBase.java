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

package com.github.sonus21.rqueue.test.tests;

import com.github.sonus21.rqueue.common.RqueueRedisTemplate;
import com.github.sonus21.rqueue.config.RqueueConfig;
import com.github.sonus21.rqueue.config.RqueueWebConfig;
import com.github.sonus21.rqueue.core.QueueRegistry;
import com.github.sonus21.rqueue.core.RqueueMessage;
import com.github.sonus21.rqueue.core.RqueueMessageSender;
import com.github.sonus21.rqueue.core.RqueueMessageTemplate;
import com.github.sonus21.rqueue.core.support.RqueueMessageFactory;
import com.github.sonus21.rqueue.listener.QueueDetail;
import com.github.sonus21.rqueue.listener.RqueueMessageListenerContainer;
import com.github.sonus21.rqueue.test.service.ConsumedMessageService;
import com.github.sonus21.rqueue.test.service.FailureManager;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;

@Slf4j
public abstract class SpringTestBase {
  @Autowired protected RqueueMessageSender rqueueMessageSender;
  @Autowired protected RqueueMessageTemplate rqueueMessageTemplate;
  @Autowired protected RqueueConfig rqueueConfig;
  @Autowired protected RqueueWebConfig rqueueWebConfig;
  @Autowired protected RqueueMessageSender messageSender;
  @Autowired protected RqueueRedisTemplate<String> stringRqueueRedisTemplate;
  @Autowired protected ConsumedMessageService consumedMessageService;
  @Autowired protected RqueueMessageListenerContainer rqueueMessageListenerContainer;
  @Autowired protected FailureManager failureManager;

  @Value("${email.queue.name}")
  protected String emailQueue;

  @Value("${email.dead.letter.queue.name}")
  protected String emailDeadLetterQueue;

  @Value("${email.queue.retry.count}")
  protected int emailRetryCount;

  @Value("${job.queue.name}")
  protected String jobQueue;

  @Value("${notification.queue.name}")
  protected String notificationQueue;

  @Value("${notification.queue.retry.count}")
  protected int notificationRetryCount;

  @Value("${sms.queue}")
  protected String smsQueue;

  @Value("${feed.generation.queue}")
  protected String feedGenerationQueue;

  @Value("${chat.indexing.queue}")
  protected String chatIndexingQueue;

  @Value("${reservation.queue}")
  protected String reservationQueue;

  protected void enqueue(Object message, String queueName) {
    RqueueMessage rqueueMessage = RqueueMessageFactory.buildMessage(message, queueName, null, null);
    rqueueMessageTemplate.addMessage(queueName, rqueueMessage);
  }

  public interface Factory {
    Object next(int i);
  }

  public interface Delay {
    long getDelay(int i);
  }

  protected void enqueue(String queueName, Factory factory, int n) {
    for (int i = 0; i < n; i++) {
      Object object = factory.next(i);
      RqueueMessage rqueueMessage =
          RqueueMessageFactory.buildMessage(object, queueName, null, null);
      rqueueMessageTemplate.addMessage(queueName, rqueueMessage);
    }
  }

  protected void enqueueIn(String zsetName, Factory factory, Delay delay, int n) {
    for (int i = 0; i < n; i++) {
      Object object = factory.next(i);
      long score = delay.getDelay(i);
      RqueueMessage rqueueMessage =
          RqueueMessageFactory.buildMessage(object, zsetName, null, score);
      rqueueMessageTemplate.addToZset(zsetName, rqueueMessage, rqueueMessage.getProcessAt());
    }
  }

  protected void enqueueIn(Object message, String zsetName, long delay) {
    RqueueMessage rqueueMessage = RqueueMessageFactory.buildMessage(message, zsetName, null, delay);
    rqueueMessageTemplate.addToZset(zsetName, rqueueMessage, rqueueMessage.getProcessAt());
  }

  protected Map<String, List<RqueueMessage>> getMessageMap(String queueName) {
    QueueDetail queueDetail = QueueRegistry.get(queueName);
    Map<String, List<RqueueMessage>> queueNameToMessage = new HashMap<>();
    List<RqueueMessage> messages =
        rqueueMessageTemplate.readFromList(queueDetail.getQueueName(), 0, -1);
    queueNameToMessage.put(queueDetail.getQueueName(), messages);

    List<RqueueMessage> messagesFromZset =
        rqueueMessageTemplate.readFromZset(queueDetail.getDelayedQueueName(), 0, -1);
    queueNameToMessage.put(queueDetail.getDelayedQueueName(), messagesFromZset);

    List<RqueueMessage> messagesInProcessingQueue =
        rqueueMessageTemplate.readFromZset(queueDetail.getProcessingQueueName(), 0, -1);
    queueNameToMessage.put(queueDetail.getProcessingQueueName(), messagesInProcessingQueue);
    return queueNameToMessage;
  }

  protected int getMessageCount(List<String> queueNames) {
    int count = 0;
    for (String queueName : queueNames) {
      for (Entry<String, List<RqueueMessage>> entry : getMessageMap(queueName).entrySet()) {
        count += entry.getValue().size();
      }
    }
    return count;
  }

  protected int getMessageCount(String queueName) {
    return getMessageCount(Collections.singletonList(queueName));
  }

  protected void printQueueStats(List<String> queueNames) {
    for (String queueName : queueNames) {
      for (Entry<String, List<RqueueMessage>> entry : getMessageMap(queueName).entrySet()) {
        for (RqueueMessage message : entry.getValue()) {
          log.info("Queue: {} Msg: {}", entry.getKey(), message);
        }
      }
    }
  }
}
