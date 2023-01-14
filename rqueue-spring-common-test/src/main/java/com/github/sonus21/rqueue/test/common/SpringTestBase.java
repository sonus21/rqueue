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

package com.github.sonus21.rqueue.test.common;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.sonus21.TestBase;
import com.github.sonus21.rqueue.common.RqueueRedisTemplate;
import com.github.sonus21.rqueue.config.RqueueConfig;
import com.github.sonus21.rqueue.config.RqueueWebConfig;
import com.github.sonus21.rqueue.core.EndpointRegistry;
import com.github.sonus21.rqueue.core.ReactiveRqueueMessageEnqueuer;
import com.github.sonus21.rqueue.core.RqueueEndpointManager;
import com.github.sonus21.rqueue.core.RqueueMessage;
import com.github.sonus21.rqueue.core.RqueueMessageEnqueuer;
import com.github.sonus21.rqueue.core.RqueueMessageManager;
import com.github.sonus21.rqueue.core.RqueueMessageTemplate;
import com.github.sonus21.rqueue.core.support.RqueueMessageUtils;
import com.github.sonus21.rqueue.dao.RqueueJobDao;
import com.github.sonus21.rqueue.exception.TimedOutException;
import com.github.sonus21.rqueue.listener.QueueDetail;
import com.github.sonus21.rqueue.listener.RqueueMessageListenerContainer;
import com.github.sonus21.rqueue.metrics.RqueueQueueMetrics;
import com.github.sonus21.rqueue.test.entity.ConsumedMessage;
import com.github.sonus21.rqueue.test.service.ConsumedMessageStore;
import com.github.sonus21.rqueue.test.service.FailureManager;
import com.github.sonus21.rqueue.test.service.RqueueEventListener;
import com.github.sonus21.rqueue.utils.StringUtils;
import com.github.sonus21.rqueue.utils.TimeoutUtils;
import com.github.sonus21.rqueue.web.service.RqueueMessageMetadataService;
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

  @Autowired
  protected RqueueMessageTemplate rqueueMessageTemplate;
  @Autowired
  protected RqueueConfig rqueueConfig;
  @Autowired
  protected RqueueWebConfig rqueueWebConfig;
  @Autowired
  protected RqueueRedisTemplate<String> stringRqueueRedisTemplate;
  @Autowired
  protected ConsumedMessageStore consumedMessageStore;
  @Autowired
  protected RqueueMessageListenerContainer rqueueMessageListenerContainer;
  @Autowired
  protected FailureManager failureManager;
  @Autowired
  protected RqueueMessageEnqueuer rqueueMessageEnqueuer;
  @Autowired
  protected RqueueEventListener rqueueEventListener;

  @Autowired(required = false)
  protected ReactiveRqueueMessageEnqueuer reactiveRqueueMessageEnqueuer;

  @Autowired
  protected RqueueEndpointManager rqueueEndpointManager;
  @Autowired
  protected RqueueMessageManager rqueueMessageManager;
  @Autowired
  protected RqueueJobDao rqueueJobDao;
  @Autowired
  protected RqueueMessageMetadataService rqueueMessageMetadataService;
  @Autowired
  protected ObjectMapper objectMapper;
  @Autowired
  protected RqueueQueueMetrics rqueueQueueMetrics;

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

  @Value("${periodic.job.queue.name}")
  protected String periodicJobQueue;

  @Value("${long.running.job.queue.name:}")
  protected String longRunningJobQueue;

  @Value("${rqueue.retry.per.poll:-1}")
  protected int retryPerPoll;

  @Value("${user.banned.queue.name}")
  protected String userBannedQueue;

  @Value("${rqueue.reactive.enabled:false}")
  protected boolean reactiveEnabled;

  protected void enqueue(Object message, String queueName) {
    RqueueMessage rqueueMessage =
        RqueueMessageUtils.buildMessage(
            rqueueMessageManager.getMessageConverter(), queueName, null, message, null, null, null);
    rqueueMessageTemplate.addMessage(queueName, rqueueMessage);
  }

  protected void enqueue(String queueName, Factory factory, int n, boolean useMessageTemplate) {
    for (int i = 0; i < n; i++) {
      Object message = factory.next(i);
      if (useMessageTemplate) {
        RqueueMessage rqueueMessage =
            RqueueMessageUtils.buildMessage(
                rqueueMessageManager.getMessageConverter(),
                queueName,
                null,
                message,
                null,
                null,
                rqueueMessageListenerContainer.getMessageHeaders());
        rqueueMessageTemplate.addMessage(queueName, rqueueMessage);
      } else {
        enqueue(queueName, message);
      }
    }
  }

  protected void enqueueIn(
      String queueName, Factory factory, Delay delayFunc, int n, boolean useMessageTemplate) {
    for (int i = 0; i < n; i++) {
      Object message = factory.next(i);
      long delay = delayFunc.getDelay(i);
      if (useMessageTemplate) {
        RqueueMessage rqueueMessage =
            RqueueMessageUtils.buildMessage(
                rqueueMessageManager.getMessageConverter(),
                queueName,
                null,
                message,
                null,
                delay,
                null);
        rqueueMessageTemplate.addToZset(queueName, rqueueMessage, rqueueMessage.getProcessAt());
      } else {
        enqueueIn(queueName, message, delay);
      }
    }
  }

  protected void enqueueIn(Object message, String zsetName, long delay) {
    RqueueMessage rqueueMessage =
        RqueueMessageUtils.buildMessage(
            rqueueMessageManager.getMessageConverter(), zsetName, null, message, null, delay, null);
    rqueueMessageTemplate.addToZset(zsetName, rqueueMessage, rqueueMessage.getProcessAt());
  }

  protected Map<String, List<RqueueMessage>> getMessageMap(String queueName) {
    QueueDetail queueDetail = EndpointRegistry.get(queueName);
    Map<String, List<RqueueMessage>> queueNameToMessage = new HashMap<>();
    List<RqueueMessage> messages =
        rqueueMessageTemplate.readFromList(queueDetail.getQueueName(), 0, -1);
    queueNameToMessage.put(queueDetail.getQueueName(), messages);

    List<RqueueMessage> messagesInScheduledQueue =
        rqueueMessageTemplate.readFromZset(queueDetail.getScheduledQueueName(), 0, -1);
    queueNameToMessage.put(queueDetail.getScheduledQueueName(), messagesInScheduledQueue);

    List<RqueueMessage> messagesInProcessingQueue =
        rqueueMessageTemplate.readFromZset(queueDetail.getProcessingQueueName(), 0, -1);
    queueNameToMessage.put(queueDetail.getProcessingQueueName(), messagesInProcessingQueue);
    return queueNameToMessage;
  }

  protected int getMessageCount(List<String> queueNames) {
    int count = 0;
    for (String queueName : queueNames) {
      count += rqueueQueueMetrics.getPendingMessageCount(queueName);
      count += rqueueQueueMetrics.getProcessingMessageCount(queueName);
      count += rqueueQueueMetrics.getScheduledMessageCount(queueName);
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

  protected void printQueueStats(String queueName) {
    printQueueStats(Collections.singletonList(queueName));
  }

  protected void printConsumedMessage(String queueName) {
    for (ConsumedMessage consumedMessage :
        consumedMessageStore.getConsumedMessagesForQueue(queueName)) {
      log.info("Queue {} Msg: {}", queueName, consumedMessage);
    }
  }

  protected void cleanQueue(String queue) {
    QueueDetail queueDetail = EndpointRegistry.get(queue);
    stringRqueueRedisTemplate.delete(queueDetail.getQueueName());
    stringRqueueRedisTemplate.delete(queueDetail.getScheduledQueueName());
    stringRqueueRedisTemplate.delete(queueDetail.getProcessingQueueName());
    if (!StringUtils.isEmpty(queueDetail.getDeadLetterQueueName())) {
      stringRqueueRedisTemplate.delete(queueDetail.getDeadLetterQueueName());
    }
  }

  protected boolean enqueue(String queueName, Object message) {
    if (reactiveEnabled) {
      return reactiveRqueueMessageEnqueuer.enqueue(queueName, message).block() != null;
    }
    return rqueueMessageEnqueuer.enqueue(queueName, message) != null;
  }

  protected boolean enqueueAt(String queueName, Object message, Date instant) {
    if (reactiveEnabled) {
      return reactiveRqueueMessageEnqueuer.enqueueAt(queueName, message, instant).block() != null;
    }
    return rqueueMessageEnqueuer.enqueueAt(queueName, message, instant) != null;
  }

  protected boolean enqueueAt(String queueName, Object message, Instant instant) {
    if (reactiveEnabled) {
      return reactiveRqueueMessageEnqueuer.enqueueAt(queueName, message, instant).block() != null;
    }
    return rqueueMessageEnqueuer.enqueueAt(queueName, message, instant) != null;
  }

  protected boolean enqueueAt(String queueName, Object message, long delay) {
    if (reactiveEnabled) {
      return reactiveRqueueMessageEnqueuer.enqueueAt(queueName, message, delay).block() != null;
    }
    return rqueueMessageEnqueuer.enqueueAt(queueName, message, delay) != null;
  }

  protected String enqueueAtGetMessageId(String queueName, Object message, long delay) {
    if (reactiveEnabled) {
      return reactiveRqueueMessageEnqueuer.enqueueAt(queueName, message, delay).block();
    }
    return rqueueMessageEnqueuer.enqueueAt(queueName, message, delay);
  }

  protected boolean enqueueIn(String queueName, Object message, long delay) {
    if (reactiveEnabled) {
      return reactiveRqueueMessageEnqueuer.enqueueIn(queueName, message, delay).block() != null;
    }
    return rqueueMessageEnqueuer.enqueueIn(queueName, message, delay) != null;
  }

  protected boolean enqueueIn(String queueName, Object message, long delay, TimeUnit timeUnit) {
    if (reactiveEnabled) {
      return reactiveRqueueMessageEnqueuer.enqueueIn(queueName, message, delay, timeUnit).block()
          != null;
    }
    return rqueueMessageEnqueuer.enqueueIn(queueName, message, delay, timeUnit) != null;
  }

  protected boolean enqueueIn(String queueName, Object message, Duration duration) {
    if (reactiveEnabled) {
      return reactiveRqueueMessageEnqueuer.enqueueIn(queueName, message, duration).block() != null;
    }
    return rqueueMessageEnqueuer.enqueueIn(queueName, message, duration) != null;
  }

  protected boolean enqueueWithPriority(String queueName, String priority, Object message) {
    if (reactiveEnabled) {
      return reactiveRqueueMessageEnqueuer.enqueueWithPriority(queueName, priority, message).block()
          != null;
    }
    return rqueueMessageEnqueuer.enqueueWithPriority(queueName, priority, message) != null;
  }

  protected boolean enqueueInWithPriority(
      String queueName, String priority, Object message, long delay) {
    if (reactiveEnabled) {
      return reactiveRqueueMessageEnqueuer
          .enqueueInWithPriority(queueName, priority, message, delay)
          .block()
          != null;
    }
    return rqueueMessageEnqueuer.enqueueInWithPriority(queueName, priority, message, delay) != null;
  }

  protected boolean enqueueInWithPriority(
      String queueName, String priority, Object message, long delay, TimeUnit unit) {
    if (reactiveEnabled) {
      return reactiveRqueueMessageEnqueuer
          .enqueueInWithPriority(queueName, priority, message, delay, unit)
          .block()
          != null;
    }
    return rqueueMessageEnqueuer.enqueueInWithPriority(queueName, priority, message, delay, unit)
        != null;
  }

  protected boolean enqueueInWithPriority(
      String queueName, String priority, Object message, Duration duration) {
    if (reactiveEnabled) {
      return reactiveRqueueMessageEnqueuer
          .enqueueInWithPriority(queueName, priority, message, duration)
          .block()
          != null;
    }
    return rqueueMessageEnqueuer.enqueueInWithPriority(queueName, priority, message, duration)
        != null;
  }

  protected boolean enqueueAtWithPriority(
      String queueName, String priority, Object message, Date date) {
    if (reactiveEnabled) {
      return reactiveRqueueMessageEnqueuer
          .enqueueAtWithPriority(queueName, priority, message, date)
          .block()
          != null;
    }
    return rqueueMessageEnqueuer.enqueueAtWithPriority(queueName, priority, message, date) != null;
  }

  protected boolean enqueueAtWithPriority(
      String queueName, String priority, Object message, Instant date) {
    if (reactiveEnabled) {
      return reactiveRqueueMessageEnqueuer
          .enqueueAtWithPriority(queueName, priority, message, date)
          .block()
          != null;
    }
    return rqueueMessageEnqueuer.enqueueAtWithPriority(queueName, priority, message, date) != null;
  }

  protected boolean enqueueAtWithPriority(
      String queueName, String priority, Object message, long instant) {
    if (reactiveEnabled) {
      return reactiveRqueueMessageEnqueuer
          .enqueueAtWithPriority(queueName, priority, message, instant)
          .block()
          != null;
    }
    return rqueueMessageEnqueuer.enqueueAtWithPriority(queueName, priority, message, instant)
        != null;
  }

  protected boolean enqueueWithRetry(String queueName, Object message, int retry) {
    if (reactiveEnabled) {
      return reactiveRqueueMessageEnqueuer.enqueueWithRetry(queueName, message, retry).block()
          != null;
    }
    return rqueueMessageEnqueuer.enqueueWithRetry(queueName, message, retry) != null;
  }

  protected void registerQueue(String queue, String... priorities) {
    rqueueEndpointManager.registerQueue(queue, priorities);
  }

  protected List<RqueueMessage> getProcessingMessages(String queueName) {
    return rqueueMessageTemplate.readFromZset(
        EndpointRegistry.get(queueName).getProcessingQueueName(), 0, -1);
  }

  protected List<Object> getAllMessages(String queueName) {
    return rqueueMessageManager.getAllMessages(queueName);
  }

  private void deleteAllMessageInternal(String queueName) throws TimedOutException {
    rqueueMessageManager.deleteAllMessages(queueName);
    TimeoutUtils.waitFor(() -> getMessageCount(queueName) == 0, "message deletion");
  }

  protected void deleteAllMessages(String queueName) throws TimedOutException {
    int i = 0;
    while (true) {
      try {
        deleteAllMessageInternal(queueName);
        return;
      } catch (TimedOutException e) {
        if (i == 2) {
          throw e;
        }
      }
      TimeoutUtils.sleep(1000);
      i++;
    }
  }

  public interface Factory {

    Object next(int i);
  }

  public interface Delay {

    long getDelay(int i);
  }
}
