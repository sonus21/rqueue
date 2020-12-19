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

import static com.github.sonus21.rqueue.core.support.RqueueMessageUtils.buildMessage;
import static com.github.sonus21.rqueue.core.support.RqueueMessageUtils.buildPeriodicMessage;
import static com.github.sonus21.rqueue.utils.Constants.MIN_DELAY;
import static com.github.sonus21.rqueue.utils.Validator.validateQueue;
import static org.springframework.util.Assert.notNull;

import com.github.sonus21.rqueue.config.RqueueConfig;
import com.github.sonus21.rqueue.core.EndpointRegistry;
import com.github.sonus21.rqueue.core.RqueueMessage;
import com.github.sonus21.rqueue.core.RqueueMessageTemplate;
import com.github.sonus21.rqueue.core.support.RqueueMessageUtils;
import com.github.sonus21.rqueue.dao.RqueueStringDao;
import com.github.sonus21.rqueue.listener.QueueDetail;
import com.github.sonus21.rqueue.models.db.MessageMetadata;
import com.github.sonus21.rqueue.models.db.MessageStatus;
import com.github.sonus21.rqueue.utils.PriorityUtils;
import com.github.sonus21.rqueue.web.service.RqueueMessageMetadataService;
import java.time.Duration;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.ListUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.messaging.MessageHeaders;
import org.springframework.messaging.converter.MessageConverter;
import org.springframework.util.CollectionUtils;

@Slf4j
@SuppressWarnings("WeakerAccess")
abstract class BaseMessageSender {
  protected final MessageHeaders messageHeaders;
  protected MessageConverter messageConverter;
  protected RqueueMessageTemplate messageTemplate;
  @Autowired protected RqueueStringDao rqueueStringDao;
  @Autowired protected RqueueConfig rqueueConfig;
  @Autowired protected RqueueMessageMetadataService rqueueMessageMetadataService;
  private ExecutorService executorService;

  BaseMessageSender(
      RqueueMessageTemplate messageTemplate,
      MessageConverter messageConverter,
      MessageHeaders messageHeaders,
      boolean shouldCreateExecutor) {
    notNull(messageTemplate, "messageTemplate cannot be null");
    notNull(messageConverter, "messageConverter cannot be null");
    this.messageTemplate = messageTemplate;
    this.messageConverter = messageConverter;
    this.messageHeaders = messageHeaders;
    if (shouldCreateExecutor) {
      this.executorService = Executors.newSingleThreadExecutor();
    }
  }

  protected void storeMessageMetadata(RqueueMessage rqueueMessage, Long delayInMillis) {
    MessageMetadata messageMetadata = new MessageMetadata(rqueueMessage, MessageStatus.ENQUEUED);
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
        buildMessage(
            messageConverter, queueName, message, retryCount, delayInMilliSecs, messageHeaders);
    if (messageId != null) {
      rqueueMessage.setId(messageId);
    }
    return rqueueMessage;
  }

  protected void enqueue(
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

  protected String pushPeriodicMessage(
      String queueName, String messageId, Object message, long periodInMilliSeconds) {
    QueueDetail queueDetail = EndpointRegistry.get(queueName);
    RqueueMessage rqueueMessage =
        buildPeriodicMessage(
            messageConverter, queueName, message, periodInMilliSeconds, messageHeaders);
    if (messageId != null) {
      rqueueMessage.setId(messageId);
    }
    try {
      enqueue(queueDetail, rqueueMessage, periodInMilliSeconds);
      storeMessageMetadata(rqueueMessage, periodInMilliSeconds);
    } catch (Exception e) {
      log.error("Queue: {} Message {} could not be pushed {}", queueName, rqueueMessage, e);
      return null;
    }
    return rqueueMessage.getId();
  }

  private List<String> getMessageIdFromList(String queueName) {
    long batchSize = 1000;
    long offset = 0;
    List<String> ids = new LinkedList<>();
    while (true) {
      List<RqueueMessage> rqueueMessageList =
          messageTemplate.readFromList(queueName, offset, batchSize);
      if (!CollectionUtils.isEmpty(rqueueMessageList)) {
        for (RqueueMessage rqueueMessage : rqueueMessageList) {
          ids.add(rqueueMessage.getId());
        }
      }
      if (CollectionUtils.isEmpty(rqueueMessageList) || rqueueMessageList.size() < batchSize) {
        break;
      }
      offset += batchSize;
    }
    return ids;
  }

  private List<String> getMessageIdFromZset(String zsetName) {
    List<String> ids = new LinkedList<>();
    List<RqueueMessage> rqueueMessageList = messageTemplate.readFromZset(zsetName, 0, -1);
    if (!CollectionUtils.isEmpty(rqueueMessageList)) {
      for (RqueueMessage rqueueMessage : rqueueMessageList) {
        ids.add(rqueueMessage.getId());
      }
    }
    return ids;
  }

  protected Object deleteAllMessages(QueueDetail queueDetail) {
    List<String> messageIds = getMessageIdFromList(queueDetail.getQueueName());
    List<String> messageIdFromProcessingSet =
        getMessageIdFromZset(queueDetail.getProcessingQueueName());
    List<String> messageIdFromDelayedSet = getMessageIdFromZset(queueDetail.getDelayedQueueName());
    messageIds.addAll(messageIdFromProcessingSet);
    messageIds.addAll(messageIdFromDelayedSet);
    Object deleted =
        rqueueStringDao.delete(
            Arrays.asList(
                queueDetail.getQueueName(),
                queueDetail.getProcessingQueueName(),
                queueDetail.getDelayedQueueName()));
    if (!CollectionUtils.isEmpty(messageIds)) {
      executorService.submit(new MessageDeleteJob(messageIds, queueDetail));
    }
    return deleted;
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

  private class MessageDeleteJob implements Runnable {
    private static final int batchSize = 1000;
    private final QueueDetail queueDetail;
    private final List<String> ids;

    MessageDeleteJob(List<String> ids, QueueDetail queueDetail) {
      this.ids = ids;
      this.queueDetail = queueDetail;
    }

    @Override
    public void run() {
      for (List<String> subIds : ListUtils.partition(ids, batchSize)) {
        List<String> messageMetaIds =
            subIds.stream()
                .map(e -> RqueueMessageUtils.getMessageMetaId(queueDetail.getName(), e))
                .collect(Collectors.toList());
        rqueueStringDao.delete(messageMetaIds);
        log.info("Deleted {} messages meta", messageMetaIds.size());
      }
    }
  }
}
