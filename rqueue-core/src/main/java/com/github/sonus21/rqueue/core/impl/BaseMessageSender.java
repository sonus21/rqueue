/*
 * Copyright (c) 2020-2025 Sonu Kumar
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

import static com.github.sonus21.rqueue.core.support.RqueueMessageUtils.buildMessage;
import static com.github.sonus21.rqueue.core.support.RqueueMessageUtils.buildPeriodicMessage;
import static com.github.sonus21.rqueue.utils.Constants.DEFAULT_PRIORITY_KEY;
import static com.github.sonus21.rqueue.utils.Constants.MIN_DELAY;
import static com.github.sonus21.rqueue.utils.Validator.validateQueue;
import static org.springframework.util.Assert.notNull;

import com.github.sonus21.rqueue.config.RqueueConfig;
import com.github.sonus21.rqueue.core.EndpointRegistry;
import com.github.sonus21.rqueue.core.RqueueMessage;
import com.github.sonus21.rqueue.core.RqueueMessageTemplate;
import com.github.sonus21.rqueue.core.impl.MessageSweeper.MessageDeleteRequest;
import com.github.sonus21.rqueue.dao.RqueueStringDao;
import com.github.sonus21.rqueue.exception.DuplicateMessageException;
import com.github.sonus21.rqueue.listener.QueueDetail;
import com.github.sonus21.rqueue.models.db.MessageMetadata;
import com.github.sonus21.rqueue.models.enums.MessageStatus;
import com.github.sonus21.rqueue.utils.PriorityUtils;
import com.github.sonus21.rqueue.web.service.RqueueMessageMetadataService;
import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.messaging.MessageHeaders;
import org.springframework.messaging.converter.MessageConverter;

@Slf4j
@SuppressWarnings("WeakerAccess")
abstract class BaseMessageSender {

  protected final MessageHeaders messageHeaders;
  protected final MessageConverter messageConverter;
  protected final RqueueMessageTemplate messageTemplate;
  @Autowired protected RqueueStringDao rqueueStringDao;
  @Autowired protected RqueueConfig rqueueConfig;
  @Autowired protected RqueueMessageMetadataService rqueueMessageMetadataService;

  BaseMessageSender(
      RqueueMessageTemplate messageTemplate,
      MessageConverter messageConverter,
      MessageHeaders messageHeaders) {
    notNull(messageTemplate, "messageTemplate cannot be null");
    notNull(messageConverter, "messageConverter cannot be null");
    this.messageTemplate = messageTemplate;
    this.messageConverter = messageConverter;
    this.messageHeaders = messageHeaders;
  }

  protected Object storeMessageMetadata(
      RqueueMessage rqueueMessage, Long delayInMillis, boolean reactive, boolean isUnique) {
    MessageMetadata messageMetadata = new MessageMetadata(rqueueMessage, MessageStatus.ENQUEUED);
    Duration duration = rqueueConfig.getMessageDurability(delayInMillis);
    if (reactive) {
      return rqueueMessageMetadataService.saveReactive(messageMetadata, duration, isUnique);
    } else {
      rqueueMessageMetadataService.save(messageMetadata, duration, isUnique);
    }
    return null;
  }

  protected Object enqueue(
      QueueDetail queueDetail,
      RqueueMessage rqueueMessage,
      Long delayInMilliSecs,
      boolean reactive) {
    if (delayInMilliSecs == null || delayInMilliSecs <= MIN_DELAY) {
      if (reactive) {
        return messageTemplate.addReactiveMessage(queueDetail.getQueueName(), rqueueMessage);
      } else {
        messageTemplate.addMessage(queueDetail.getQueueName(), rqueueMessage);
      }
    } else {
      if (reactive) {
        return messageTemplate.addReactiveMessageWithDelay(
            queueDetail.getScheduledQueueName(),
            queueDetail.getScheduledQueueChannelName(),
            rqueueMessage);
      } else {
        messageTemplate.addMessageWithDelay(
            queueDetail.getScheduledQueueName(),
            queueDetail.getScheduledQueueChannelName(),
            rqueueMessage);
      }
    }
    return null;
  }

  protected String pushMessage(
      String queueName,
      String messageId,
      Object message,
      Integer retryCount,
      Long delayInMilliSecs,
      boolean isUnique) {
    QueueDetail queueDetail = EndpointRegistry.get(queueName);
    RqueueMessage rqueueMessage =
        buildMessage(
            messageConverter,
            queueName,
            messageId,
            message,
            retryCount,
            delayInMilliSecs,
            messageHeaders);
    try {
      storeMessageMetadata(rqueueMessage, delayInMilliSecs, false, isUnique);
      enqueue(queueDetail, rqueueMessage, delayInMilliSecs, false);
    } catch (DuplicateMessageException e) {
      log.warn(
          "Duplicate message enqueue attempted queue: {}, messageId: {}",
          queueName,
          rqueueMessage.getId());
      return null;
    } catch (Exception e) {
      log.error("Queue: {} Message {} could not be pushed", queueName, rqueueMessage.getId(), e);
      return null;
    }
    return rqueueMessage.getId();
  }

  protected String pushPeriodicMessage(
      String queueName,
      String messageId,
      Object message,
      long periodInMilliSeconds) {
    QueueDetail queueDetail = EndpointRegistry.get(queueName);
    RqueueMessage rqueueMessage =
        buildPeriodicMessage(
            messageConverter,
            queueName,
            messageId,
            message,
            null,
            periodInMilliSeconds,
            messageHeaders);
    try {
      storeMessageMetadata(rqueueMessage, periodInMilliSeconds, false, false);
      enqueue(queueDetail, rqueueMessage, periodInMilliSeconds, false);
      return rqueueMessage.getId();
    } catch (Exception e) {
      log.error("Queue: {} Message {} could not be pushed", queueName, rqueueMessage, e);
      return null;
    }
  }

  protected Object deleteAllMessages(QueueDetail queueDetail) {
    return MessageSweeper.getInstance(rqueueConfig, messageTemplate, rqueueMessageMetadataService)
        .deleteAllMessages(MessageDeleteRequest.builder().queueDetail(queueDetail).build());
  }

  protected void registerQueueInternal(String queueName, String... priorities) {
    validateQueue(queueName);
    notNull(priorities, "priorities cannot be null");
    Map<String, Integer> priorityMap = new HashMap<>();
    priorityMap.put(DEFAULT_PRIORITY_KEY, 1);
    for (String priority : priorities) {
      priorityMap.put(priority, 1);
    }

    QueueDetail queueDetail =
        QueueDetail.builder()
            .name(queueName)
            .active(false)
            .queueName(rqueueConfig.getQueueName(queueName))
            .scheduledQueueName(rqueueConfig.getScheduledQueueName(queueName))
            .scheduledQueueChannelName(rqueueConfig.getScheduledQueueChannelName(queueName))
            .processingQueueName(rqueueConfig.getProcessingQueueName(queueName))
            .processingQueueChannelName(rqueueConfig.getProcessingQueueChannelName(queueName))
            .priority(priorityMap)
            .build();
    EndpointRegistry.register(queueDetail);
    for (String priority : priorities) {
      String suffix = PriorityUtils.getSuffix(priority);
      queueDetail =
          QueueDetail.builder()
              .name(queueName + suffix)
              .active(false)
              .queueName(rqueueConfig.getQueueName(queueName) + suffix)
              .scheduledQueueName(rqueueConfig.getScheduledQueueName(queueName) + suffix)
              .scheduledQueueChannelName(
                  rqueueConfig.getScheduledQueueChannelName(queueName) + suffix)
              .processingQueueName(rqueueConfig.getProcessingQueueName(queueName) + suffix)
              .processingQueueChannelName(
                  rqueueConfig.getProcessingQueueChannelName(queueName) + suffix)
              .priority(Collections.singletonMap(DEFAULT_PRIORITY_KEY, 1))
              .build();
      EndpointRegistry.register(queueDetail);
    }
  }
}
