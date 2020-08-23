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

import static com.github.sonus21.rqueue.utils.Constants.MIN_DELAY;
import static com.github.sonus21.rqueue.utils.MessageUtils.buildMessage;
import static com.github.sonus21.rqueue.utils.Validator.validateQueue;
import static org.springframework.util.Assert.notNull;

import com.github.sonus21.rqueue.common.RqueueRedisTemplate;
import com.github.sonus21.rqueue.config.RqueueConfig;
import com.github.sonus21.rqueue.converter.GenericMessageConverter;
import com.github.sonus21.rqueue.core.EndpointRegistry;
import com.github.sonus21.rqueue.core.RqueueMessage;
import com.github.sonus21.rqueue.core.RqueueMessageTemplate;
import com.github.sonus21.rqueue.listener.QueueDetail;
import com.github.sonus21.rqueue.models.db.MessageMetadata;
import com.github.sonus21.rqueue.utils.PriorityUtils;
import com.github.sonus21.rqueue.web.service.RqueueMessageMetadataService;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.messaging.converter.CompositeMessageConverter;
import org.springframework.messaging.converter.MessageConverter;
import org.springframework.messaging.converter.StringMessageConverter;

@Slf4j
@SuppressWarnings("WeakerAccess")
abstract class BaseMessageSender {
  protected CompositeMessageConverter messageConverter;
  protected RqueueMessageTemplate messageTemplate;
  @Autowired protected RqueueRedisTemplate<String> stringRqueueRedisTemplate;
  @Autowired protected RqueueConfig rqueueConfig;
  @Autowired protected RqueueMessageMetadataService rqueueMessageMetadataService;

  BaseMessageSender(RqueueMessageTemplate messageTemplate) {
    notNull(messageTemplate, "messageTemplate cannot be null");
    this.messageTemplate = messageTemplate;
  }

  protected void init(List<MessageConverter> messageConverters, boolean addDefault) {
    this.messageConverter =
        new CompositeMessageConverter(getMessageConverters(addDefault, messageConverters));
  }

  private void storeMessageMetadata(RqueueMessage rqueueMessage, Long delayInMillis) {
    MessageMetadata messageMetadata = new MessageMetadata(rqueueMessage);
    Duration duration;
    if (delayInMillis != null) {
      duration = Duration.ofMillis(2 * delayInMillis);
      long minutes = duration.toMinutes();
      if (minutes < rqueueConfig.getMessageDurabilityInMinute()) {
        duration = Duration.ofMinutes(rqueueConfig.getMessageDurabilityInMinute());
      }
    } else {
      duration = Duration.ofMinutes(rqueueConfig.getMessageDurabilityInMinute());
    }
    rqueueMessageMetadataService.save(messageMetadata, duration);
  }

  private RqueueMessage constructMessage(
      String queueName,
      String messageId,
      Object message,
      Integer retryCount,
      Long delayInMilliSecs) {
    RqueueMessage rqueueMessage =
        buildMessage(messageConverter, queueName, message, retryCount, delayInMilliSecs);
    if (messageId != null) {
      rqueueMessage.setId(messageId);
    }
    return rqueueMessage;
  }

  private void enqueue(
      QueueDetail queueDetail, RqueueMessage rqueueMessage, Long delayInMilliSecs) {
    if (delayInMilliSecs == null || delayInMilliSecs <= MIN_DELAY) {
      messageTemplate.addMessage(queueDetail.getQueueName(), rqueueMessage);
    } else {
      messageTemplate.addMessageWithDelay(
          queueDetail.getDelayedQueueName(),
          queueDetail.getDelayedQueueChannelName(),
          rqueueMessage);
    }
  }

  protected String pushMessage(
      String queueName,
      String messageId,
      Object message,
      Integer retryCount,
      Long delayInMilliSecs) {
    QueueDetail queueDetail = EndpointRegistry.get(queueName);
    RqueueMessage rqueueMessage =
        constructMessage(queueName, messageId, message, retryCount, delayInMilliSecs);
    try {
      enqueue(queueDetail, rqueueMessage, delayInMilliSecs);
      storeMessageMetadata(rqueueMessage, delayInMilliSecs);
    } catch (Exception e) {
      log.error("Queue: {} Message {} could not be pushed {}", queueName, rqueueMessage, e);
      return null;
    }
    return rqueueMessage.getId();
  }

  protected List<MessageConverter> getMessageConverters(
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

  protected void registerQueueInternal(String queueName, String... priorities) {
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
    EndpointRegistry.register(queueDetail);
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
      EndpointRegistry.register(queueDetail);
    }
  }
}
