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

package com.github.sonus21.rqueue.core.impl;

import static org.springframework.util.Assert.isTrue;
import static org.springframework.util.Assert.notNull;

import com.github.sonus21.rqueue.common.RqueueLockManager;
import com.github.sonus21.rqueue.core.EndpointRegistry;
import com.github.sonus21.rqueue.core.RqueueMessage;
import com.github.sonus21.rqueue.core.RqueueMessageManager;
import com.github.sonus21.rqueue.core.RqueueMessageTemplate;
import com.github.sonus21.rqueue.core.support.RqueueMessageUtils;
import com.github.sonus21.rqueue.exception.LockCanNotBeAcquired;
import com.github.sonus21.rqueue.listener.QueueDetail;
import com.github.sonus21.rqueue.listener.RqueueMessageHeaders;
import com.github.sonus21.rqueue.models.MessageMoveResult;
import com.github.sonus21.rqueue.models.db.MessageMetadata;
import com.github.sonus21.rqueue.utils.Constants;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageHeaders;
import org.springframework.messaging.converter.MessageConverter;
import org.springframework.messaging.support.MessageBuilder;

@Slf4j
public class RqueueMessageManagerImpl extends BaseMessageSender implements RqueueMessageManager {

  @Autowired
  private RqueueLockManager rqueueLockManager;

  public RqueueMessageManagerImpl(
      RqueueMessageTemplate messageTemplate,
      MessageConverter messageConverter,
      MessageHeaders messageHeaders) {
    super(messageTemplate, messageConverter, messageHeaders);
  }

  @Override
  public boolean deleteAllMessages(String queueName) {
    QueueDetail queueDetail = EndpointRegistry.get(queueName);
    try {
      deleteAllMessages(queueDetail);
      return true;
    } catch (Exception e) {
      log.error("Delete all message failed", e);
      return false;
    }
  }

  @Override
  public List<Object> getAllMessages(String queueName) {
    List<Object> messages = new ArrayList<>();
    for (RqueueMessage message : getAllRqueueMessage(queueName)) {
      messages.add(RqueueMessageUtils.convertMessageToObject(message, messageConverter));
    }
    return messages;
  }

  @Override
  public Object getMessage(String queueName, String id) {
    RqueueMessage rqueueMessage = getRqueueMessage(queueName, id);
    if (rqueueMessage == null) {
      return null;
    }
    Message<String> message =
        MessageBuilder.createMessage(
            rqueueMessage.getMessage(), RqueueMessageHeaders.emptyMessageHeaders());
    return messageConverter.fromMessage(message, null);
  }

  @Override
  public RqueueMessage getRqueueMessage(String queueName, String id) {
    MessageMetadata messageMetadata = rqueueMessageMetadataService.getByMessageId(queueName, id);
    if (messageMetadata == null) {
      return null;
    }
    return messageMetadata.getRqueueMessage();
  }

  @Override
  public List<RqueueMessage> getAllRqueueMessage(String queueName) {
    QueueDetail queueDetail = EndpointRegistry.get(queueName);
    return messageTemplate.getAllMessages(
        queueDetail.getQueueName(),
        queueDetail.getProcessingQueueName(),
        queueDetail.getScheduledQueueName());
  }

  @Override
  public boolean exist(String queueName, String id) {
    String lockValue = UUID.randomUUID().toString();
    if (rqueueLockManager.acquireLock(queueName, lockValue, Duration.ofSeconds(1))) {
      boolean exist = getMessage(queueName, id) != null;
      rqueueLockManager.releaseLock(queueName, lockValue);
      return exist;
    }
    throw new LockCanNotBeAcquired(queueName);
  }

  @Override
  public boolean deleteMessage(String queueName, String id) {
    RqueueMessage rqueueMessage = getRqueueMessage(queueName, id);
    if (rqueueMessage == null) {
      return false;
    }
    Duration duration = rqueueConfig.getMessageDurability(rqueueMessage.getPeriod());
    return rqueueMessageMetadataService.deleteMessage(queueName, id, duration);
  }

  @Override
  public MessageConverter getMessageConverter() {
    return messageConverter;
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
}
