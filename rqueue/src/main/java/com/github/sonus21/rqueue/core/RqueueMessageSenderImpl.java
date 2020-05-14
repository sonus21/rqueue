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

package com.github.sonus21.rqueue.core;

import static com.github.sonus21.rqueue.utils.Constants.MIN_DELAY;
import static com.github.sonus21.rqueue.utils.MessageUtils.buildMessage;
import static com.github.sonus21.rqueue.utils.Validator.validateDelay;
import static com.github.sonus21.rqueue.utils.Validator.validateMessage;
import static com.github.sonus21.rqueue.utils.Validator.validatePriority;
import static com.github.sonus21.rqueue.utils.Validator.validateQueue;
import static com.github.sonus21.rqueue.utils.Validator.validateRetryCount;
import static org.springframework.util.Assert.isTrue;
import static org.springframework.util.Assert.notEmpty;
import static org.springframework.util.Assert.notNull;

import com.github.sonus21.rqueue.common.RqueueRedisTemplate;
import com.github.sonus21.rqueue.config.RqueueConfig;
import com.github.sonus21.rqueue.converter.GenericMessageConverter;
import com.github.sonus21.rqueue.listener.QueueDetail;
import com.github.sonus21.rqueue.models.MessageMoveResult;
import com.github.sonus21.rqueue.utils.Constants;
import com.github.sonus21.rqueue.utils.MessageUtils;
import com.github.sonus21.rqueue.utils.PriorityUtils;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.messaging.converter.CompositeMessageConverter;
import org.springframework.messaging.converter.MessageConverter;
import org.springframework.messaging.converter.StringMessageConverter;

@Slf4j
public class RqueueMessageSenderImpl implements RqueueMessageSender {
  private final CompositeMessageConverter messageConverter;
  private RqueueMessageTemplate messageTemplate;
  @Autowired private RqueueRedisTemplate<String> stringRqueueRedisTemplate;
  @Autowired private RqueueConfig rqueueConfig;

  private RqueueMessageSenderImpl(
      RqueueMessageTemplate messageTemplate,
      List<MessageConverter> messageConverters,
      boolean addDefault) {
    notNull(messageTemplate, "messageTemplate cannot be null");
    notEmpty(messageConverters, "messageConverters cannot be empty");
    this.messageTemplate = messageTemplate;
    this.messageConverter =
        new CompositeMessageConverter(getMessageConverters(addDefault, messageConverters));
  }

  public RqueueMessageSenderImpl(RqueueMessageTemplate messageTemplate) {
    this(messageTemplate, Collections.singletonList(new GenericMessageConverter()), false);
  }

  public RqueueMessageSenderImpl(
      RqueueMessageTemplate messageTemplate, List<MessageConverter> messageConverters) {
    this(messageTemplate, messageConverters, false);
  }

  @Override
  public boolean enqueue(String queueName, Object message) {
    validateQueue(queueName);
    validateMessage(message);
    return pushMessage(queueName, message, null, null);
  }

  @Override
  public boolean enqueueWithRetry(String queueName, Object message, int retryCount) {
    validateQueue(queueName);
    validateMessage(message);
    validateRetryCount(retryCount);
    return pushMessage(queueName, message, retryCount, null);
  }

  @Override
  public boolean enqueueWithPriority(String queueName, String priority, Object message) {
    validateQueue(queueName);
    validatePriority(priority);
    validateMessage(message);
    return pushMessage(
        PriorityUtils.getQueueNameForPriority(queueName, priority), message, null, null);
  }

  @Override
  public boolean enqueueIn(String queueName, Object message, long delayInMilliSecs) {
    validateQueue(queueName);
    validateMessage(message);
    validateDelay(delayInMilliSecs);
    return pushMessage(queueName, message, null, delayInMilliSecs);
  }

  @Override
  public boolean enqueueInWithRetry(
      String queueName, Object message, int retryCount, long delayInMilliSecs) {
    validateQueue(queueName);
    validateMessage(message);
    validateRetryCount(retryCount);
    validateDelay(delayInMilliSecs);
    return pushMessage(queueName, message, retryCount, delayInMilliSecs);
  }

  @Override
  public boolean enqueueInWithPriority(
      String queueName, String priority, Object message, long delayInMilliSecs) {
    validateQueue(queueName);
    validatePriority(priority);
    validateMessage(message);
    validateDelay(delayInMilliSecs);
    return pushMessage(
        PriorityUtils.getQueueNameForPriority(queueName, priority),
        message,
        null,
        delayInMilliSecs);
  }

  @Override
  public List<Object> getAllMessages(String queueName) {
    List<Object> messages = new ArrayList<>();
    QueueDetail queueDetail = QueueRegistry.get(queueName);
    for (RqueueMessage message :
        messageTemplate.getAllMessages(
            queueDetail.getQueueName(),
            queueDetail.getProcessingQueueName(),
            queueDetail.getDelayedQueueName())) {
      messages.add(MessageUtils.convertMessageToObject(message, messageConverter));
    }
    return messages;
  }

  @Override
  public MessageConverter getMessageConverter() {
    return messageConverter;
  }

  @Override
  public List<MessageConverter> getMessageConverters() {
    return messageConverter.getConverters();
  }

  @Override
  public boolean moveMessageFromDeadLetterToQueue(
      String deadLetterQueueName, String queueName, Integer maxMessages) {
    return moveMessageListToList(deadLetterQueueName, queueName, maxMessages).isSuccess();
  }

  @Override
  public boolean moveMessageFromDeadLetterToQueue(String deadLetterQueueName, String queueName) {
    return moveMessageListToList(deadLetterQueueName, queueName, null).isSuccess();
  }

  private MessageMoveResult moveMessageListToList(
      String sourceQueue, String destinationQueue, Integer maxMessage) {
    notNull(sourceQueue, "sourceQueue must not be null");
    notNull(destinationQueue, "destinationQueue must not be null");
    isTrue(
        !sourceQueue.equals(destinationQueue),
        "sourceQueue and destinationQueue must be different");
    Integer messageCount = maxMessage;
    if (messageCount == null) {
      messageCount = Constants.MAX_MESSAGES;
    }
    isTrue(messageCount > 0, "maxMessage must be greater than zero");
    return messageTemplate.moveMessageListToList(sourceQueue, destinationQueue, messageCount);
  }

  @Override
  public boolean deleteAllMessages(String queueName) {
    QueueDetail queueDetail = QueueRegistry.get(queueName);
    stringRqueueRedisTemplate.delete(queueDetail.getQueueName());
    stringRqueueRedisTemplate.delete(queueDetail.getProcessingQueueName());
    stringRqueueRedisTemplate.delete(queueDetail.getDelayedQueueName());
    return true;
  }

  private boolean pushMessage(
      String queueName, Object message, Integer retryCount, Long delayInMilliSecs) {
    QueueDetail queueDetail = QueueRegistry.get(queueName);
    RqueueMessage rqueueMessage =
        buildMessage(
            messageConverter, queueDetail.getQueueName(), message, retryCount, delayInMilliSecs);
    try {
      if (delayInMilliSecs == null || delayInMilliSecs <= MIN_DELAY) {
        messageTemplate.addMessage(queueDetail.getQueueName(), rqueueMessage);
      } else {
        messageTemplate.addMessageWithDelay(
            queueDetail.getDelayedQueueName(),
            queueDetail.getDelayedQueueChannelName(),
            rqueueMessage);
      }
    } catch (Exception e) {
      log.error("Queue: {} Message {} could not be pushed {}", queueName, rqueueMessage, e);
      return false;
    }
    return true;
  }

  private List<MessageConverter> getMessageConverters(
      boolean addDefault, List<MessageConverter> messageConverters) {
    List<MessageConverter> messageConverterList = new ArrayList<>();
    StringMessageConverter stringMessageConverter = new StringMessageConverter();
    stringMessageConverter.setSerializedPayloadClass(String.class);
    messageConverterList.add(stringMessageConverter);
    if (addDefault) {
      messageConverterList.add(new GenericMessageConverter());
    }
    messageConverterList.addAll(messageConverters);
    return messageConverterList;
  }

  @Override
  public void registerQueue(String queueName, String... priorities) {
    validateQueue(queueName);
    notNull(priorities, "priorities cannot be null");
    QueueDetail queueDetail =
        QueueDetail.builder()
            .name(queueName)
            .active(false)
            .queueName(rqueueConfig.getQueueName(queueName))
            .delayedQueueName(rqueueConfig.getDelayedQueueName(queueName))
            .delayedQueueChannelName(rqueueConfig.getDelayedQueueChannelName(queueName))
            .processingQueueName(rqueueConfig.getProcessingQueueName(queueName))
            .processingQueueChannelName(rqueueConfig.getProcessingQueueChannelName(queueName))
            .build();
    QueueRegistry.register(queueDetail);
    for (String priority : priorities) {
      String suffix = PriorityUtils.getSuffix(priority);
      queueDetail =
          QueueDetail.builder()
              .name(queueName + suffix)
              .active(false)
              .queueName(rqueueConfig.getQueueName(queueName) + suffix)
              .delayedQueueName(rqueueConfig.getDelayedQueueName(queueName) + suffix)
              .delayedQueueChannelName(rqueueConfig.getDelayedQueueChannelName(queueName) + suffix)
              .processingQueueName(rqueueConfig.getProcessingQueueName(queueName) + suffix)
              .processingQueueChannelName(
                  rqueueConfig.getProcessingQueueChannelName(queueName) + suffix)
              .build();
      QueueRegistry.register(queueDetail);
    }
  }
}
