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

package com.github.sonus21.rqueue.test.common;

import com.github.sonus21.rqueue.common.RqueueRedisTemplate;
import com.github.sonus21.rqueue.config.RqueueConfig;
import com.github.sonus21.rqueue.config.RqueueWebConfig;
import com.github.sonus21.rqueue.core.EndpointRegistry;
import com.github.sonus21.rqueue.core.RqueueEndpointManager;
import com.github.sonus21.rqueue.core.RqueueMessage;
import com.github.sonus21.rqueue.core.RqueueMessageEnqueuer;
import com.github.sonus21.rqueue.core.RqueueMessageManager;
import com.github.sonus21.rqueue.core.RqueueMessageSender;
import com.github.sonus21.rqueue.core.RqueueMessageTemplate;
import com.github.sonus21.rqueue.core.support.RqueueMessageFactory;
import com.github.sonus21.rqueue.listener.QueueDetail;
import com.github.sonus21.rqueue.listener.RqueueMessageListenerContainer;
import com.github.sonus21.rqueue.test.service.ConsumedMessageService;
import com.github.sonus21.rqueue.test.service.FailureManager;
import com.github.sonus21.rqueue.utils.StringUtils;
import java.time.Duration;
import java.time.Instant;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;

@Slf4j
public abstract class SpringTestBase extends TestBase {
  @Autowired protected RqueueMessageSender rqueueMessageSender;
  @Autowired protected RqueueMessageTemplate rqueueMessageTemplate;
  @Autowired protected RqueueConfig rqueueConfig;
  @Autowired protected RqueueWebConfig rqueueWebConfig;
  @Autowired protected RqueueRedisTemplate<String> stringRqueueRedisTemplate;
  @Autowired protected ConsumedMessageService consumedMessageService;
  @Autowired protected RqueueMessageListenerContainer rqueueMessageListenerContainer;
  @Autowired protected FailureManager failureManager;
  @Autowired protected RqueueMessageEnqueuer rqueueMessageEnqueuer;
  @Autowired protected RqueueEndpointManager rqueueEndpointManager;
  @Autowired protected RqueueMessageManager rqueueMessageManager;

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

  @Value("${reservation.request.dead.letter.queue.name}")
  protected String reservationRequestDeadLetterQueue;

  @Value("${reservation.request.queue.name}")
  protected String reservationRequestQueue;

  @Value("${reservation.request.queue.retry.count}")
  protected int reservationRequestQueueRetryCount;

  @Value("${list.email.queue.name}")
  protected String listEmailQueue;

  protected void enqueue(Object message, String queueName) {
    RqueueMessage rqueueMessage =
        RqueueMessageFactory.buildMessage(
            rqueueMessageManager.getMessageConverter(), message, queueName, null, null);
    rqueueMessageTemplate.addMessage(queueName, rqueueMessage);
  }

  protected void enqueue(String queueName, Factory factory, int n) {
    for (int i = 0; i < n; i++) {
      Object object = factory.next(i);
      RqueueMessage rqueueMessage =
          RqueueMessageFactory.buildMessage(
              rqueueMessageManager.getMessageConverter(), object, queueName, null, null);
      rqueueMessageTemplate.addMessage(queueName, rqueueMessage);
    }
  }

  protected void enqueueIn(String zsetName, Factory factory, Delay delay, int n) {
    for (int i = 0; i < n; i++) {
      Object object = factory.next(i);
      long score = delay.getDelay(i);
      RqueueMessage rqueueMessage =
          RqueueMessageFactory.buildMessage(
              rqueueMessageManager.getMessageConverter(), object, zsetName, null, score);
      rqueueMessageTemplate.addToZset(zsetName, rqueueMessage, rqueueMessage.getProcessAt());
    }
  }

  protected void enqueueIn(Object message, String zsetName, long delay) {
    RqueueMessage rqueueMessage =
        RqueueMessageFactory.buildMessage(
            rqueueMessageManager.getMessageConverter(), message, zsetName, null, delay);
    rqueueMessageTemplate.addToZset(zsetName, rqueueMessage, rqueueMessage.getProcessAt());
  }

  protected Map<String, List<RqueueMessage>> getMessageMap(String queueName) {
    QueueDetail queueDetail = EndpointRegistry.get(queueName);
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

  protected int getMessageCount(String queueName, String priority) {
    return rqueueMessageManager.getAllMessages(queueName, priority).size();
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

  protected void cleanQueue(String queue) {
    QueueDetail queueDetail = EndpointRegistry.get(queue);
    stringRqueueRedisTemplate.delete(queueDetail.getQueueName());
    stringRqueueRedisTemplate.delete(queueDetail.getDelayedQueueName());
    stringRqueueRedisTemplate.delete(queueDetail.getProcessingQueueName());
    if (!StringUtils.isEmpty(queueDetail.getDeadLetterQueueName())) {
      stringRqueueRedisTemplate.delete(queueDetail.getDeadLetterQueueName());
    }
  }

  protected boolean enqueue(String queueName, Object message) {
    if (random.nextBoolean()) {
      return rqueueMessageSender.enqueue(queueName, message);
    } else {
      return rqueueMessageEnqueuer.enqueue(queueName, message) != null;
    }
  }

  protected boolean enqueueAt(String queueName, Object message, Date instant) {
    if (random.nextBoolean()) {
      return rqueueMessageSender.enqueueAt(queueName, message, instant);
    }
    return rqueueMessageEnqueuer.enqueueAt(queueName, message, instant) != null;
  }

  protected boolean enqueueAt(String queueName, Object message, Instant instant) {
    if (random.nextBoolean()) {
      return rqueueMessageSender.enqueueAt(queueName, message, instant);
    }
    return rqueueMessageEnqueuer.enqueueAt(queueName, message, instant) != null;
  }

  protected boolean enqueueAt(String queueName, Object message, long delay) {
    if (random.nextBoolean()) {
      return rqueueMessageSender.enqueueAt(queueName, message, delay);
    }
    return rqueueMessageEnqueuer.enqueueAt(queueName, message, delay) != null;
  }

  protected void enqueueIn(String queueName, Object message, long delay) {
    if (random.nextBoolean()) {
      rqueueMessageSender.enqueueIn(queueName, message, delay);
    } else {
      rqueueMessageEnqueuer.enqueueIn(queueName, message, delay);
    }
  }

  protected void enqueueIn(String queueName, Object message, long delay, TimeUnit timeUnit) {
    if (random.nextBoolean()) {
      rqueueMessageSender.enqueueIn(queueName, message, delay, timeUnit);
    } else {
      rqueueMessageEnqueuer.enqueueIn(queueName, message, delay, timeUnit);
    }
  }

  protected boolean enqueueIn(String queueName, Object message, Duration duration) {
    if (random.nextBoolean()) {
      rqueueMessageSender.enqueueIn(queueName, message, duration);
    }
    return rqueueMessageEnqueuer.enqueueIn(queueName, message, duration) != null;
  }

  protected boolean enqueueWithPriority(String queueName, String priority, Object message) {
    if (random.nextBoolean()) {
      return rqueueMessageSender.enqueueWithPriority(queueName, priority, message);
    } else {
      return rqueueMessageEnqueuer.enqueueWithPriority(queueName, priority, message) != null;
    }
  }

  protected void enqueueInWithPriority(
      String queueName, String priority, Object message, long delay) {
    if (random.nextBoolean()) {
      rqueueMessageSender.enqueueInWithPriority(queueName, priority, message, delay);
    } else {
      rqueueMessageEnqueuer.enqueueInWithPriority(queueName, priority, message, delay);
    }
  }

  protected void enqueueInWithPriority(
      String queueName, String priority, Object message, long delay, TimeUnit unit) {
    if (random.nextBoolean()) {
      rqueueMessageSender.enqueueInWithPriority(queueName, priority, message, delay, unit);
    } else {
      rqueueMessageEnqueuer.enqueueInWithPriority(queueName, priority, message, delay, unit);
    }
  }

  protected void enqueueInWithPriority(
      String queueName, String priority, Object message, Duration duration) {
    if (random.nextBoolean()) {
      rqueueMessageSender.enqueueInWithPriority(queueName, priority, message, duration);
    } else {
      rqueueMessageEnqueuer.enqueueInWithPriority(queueName, priority, message, duration);
    }
  }

  protected boolean enqueueAtWithPriority(
      String queueName, String priority, Object message, Date date) {
    if (random.nextBoolean()) {
      return rqueueMessageSender.enqueueAtWithPriority(queueName, priority, message, date);
    } else {
      return rqueueMessageEnqueuer.enqueueAtWithPriority(queueName, priority, message, date)
          != null;
    }
  }

  protected boolean enqueueAtWithPriority(
      String queueName, String priority, Object message, Instant date) {
    if (random.nextBoolean()) {
      return rqueueMessageSender.enqueueAtWithPriority(queueName, priority, message, date);
    } else {
      return rqueueMessageEnqueuer.enqueueAtWithPriority(queueName, priority, message, date)
          != null;
    }
  }

  protected boolean enqueueAtWithPriority(
      String queueName, String priority, Object message, long instant) {
    if (random.nextBoolean()) {
      return rqueueMessageSender.enqueueAtWithPriority(queueName, priority, message, instant);
    } else {
      return rqueueMessageEnqueuer.enqueueAtWithPriority(queueName, priority, message, instant)
          != null;
    }
  }

  protected void registerQueue(String queue, String... priorities) {
    if (random.nextBoolean()) {
      rqueueMessageSender.registerQueue(queue, priorities);
    } else {
      rqueueEndpointManager.registerQueue(queue, priorities);
    }
  }

  protected boolean enqueueWithRetry(String queueName, Object message, int retry) {
    if (random.nextBoolean()) {
      return rqueueMessageSender.enqueueWithRetry(queueName, message, retry);
    }
    return rqueueMessageEnqueuer.enqueueWithRetry(queueName, message, retry) != null;
  }

  protected List<Object> getAllMessages(String queueName) {
    if (random.nextBoolean()) {
      return rqueueMessageSender.getAllMessages(queueName);
    }
    return rqueueMessageManager.getAllMessages(queueName);
  }

  protected boolean deleteAllMessages(String queueName) {
    if (random.nextBoolean()) {
      return rqueueMessageSender.deleteAllMessages(queueName);
    }
    return rqueueMessageManager.deleteAllMessages(queueName);
  }

  public interface Factory {
    Object next(int i);
  }

  public interface Delay {
    long getDelay(int i);
  }
}
