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

package com.github.sonus21.rqueue.core.impl;

import static com.github.sonus21.rqueue.utils.Validator.validateDelay;
import static com.github.sonus21.rqueue.utils.Validator.validateMessage;
import static com.github.sonus21.rqueue.utils.Validator.validatePriority;
import static com.github.sonus21.rqueue.utils.Validator.validateQueue;
import static com.github.sonus21.rqueue.utils.Validator.validateRetryCount;
import static org.springframework.util.Assert.isTrue;
import static org.springframework.util.Assert.notNull;

import com.github.sonus21.rqueue.core.EndpointRegistry;
import com.github.sonus21.rqueue.core.RqueueMessage;
import com.github.sonus21.rqueue.core.RqueueMessageSender;
import com.github.sonus21.rqueue.core.RqueueMessageTemplate;
import com.github.sonus21.rqueue.listener.QueueDetail;
import com.github.sonus21.rqueue.models.MessageMoveResult;
import com.github.sonus21.rqueue.utils.Constants;
import com.github.sonus21.rqueue.utils.MessageUtils;
import com.github.sonus21.rqueue.utils.PriorityUtils;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import com.google.common.collect.ImmutableList;
import lombok.extern.slf4j.Slf4j;
import org.springframework.messaging.converter.MessageConverter;

@Slf4j
public class RqueueMessageSenderImpl extends BaseMessageSender implements RqueueMessageSender {

  public RqueueMessageSenderImpl(
      RqueueMessageTemplate messageTemplate, MessageConverter messageConverter) {
    super(messageTemplate, messageConverter);
  }

  @Override
  public boolean enqueue(String queueName, Object message) {
    validateQueue(queueName);
    validateMessage(message);
    return pushMessage(queueName, null, message, null, null) != null;
  }

  @Override
  public boolean enqueueWithRetry(String queueName, Object message, int retryCount) {
    validateQueue(queueName);
    validateMessage(message);
    validateRetryCount(retryCount);
    return pushMessage(queueName, null, message, retryCount, null) != null;
  }

  @Override
  public boolean enqueueWithPriority(String queueName, String priority, Object message) {
    validateQueue(queueName);
    validatePriority(priority);
    validateMessage(message);
    return pushMessage(
            PriorityUtils.getQueueNameForPriority(queueName, priority), null, message, null, null)
        != null;
  }

  @Override
  public boolean enqueueIn(String queueName, Object message, long delayInMilliSecs) {
    validateQueue(queueName);
    validateMessage(message);
    validateDelay(delayInMilliSecs);
    return pushMessage(queueName, null, message, null, delayInMilliSecs) != null;
  }

  @Override
  public boolean enqueueInWithRetry(
      String queueName, Object message, int retryCount, long delayInMilliSecs) {
    validateQueue(queueName);
    validateMessage(message);
    validateRetryCount(retryCount);
    validateDelay(delayInMilliSecs);
    return pushMessage(queueName, null, message, retryCount, delayInMilliSecs) != null;
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
            null,
            message,
            null,
            delayInMilliSecs)
        != null;
  }

  @Override
  public List<Object> getAllMessages(String queueName) {
    List<Object> messages = new ArrayList<>();
    QueueDetail queueDetail = EndpointRegistry.get(queueName);
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
    return ImmutableList.of(messageConverter);
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
    QueueDetail queueDetail = EndpointRegistry.get(queueName);
    stringRqueueRedisTemplate.delete(queueDetail.getQueueName());
    stringRqueueRedisTemplate.delete(queueDetail.getProcessingQueueName());
    stringRqueueRedisTemplate.delete(queueDetail.getDelayedQueueName());
    return true;
  }

  @Override
  public void registerQueue(String queueName, String... priorities) {
    registerQueueInternal(queueName, priorities);
  }
}
