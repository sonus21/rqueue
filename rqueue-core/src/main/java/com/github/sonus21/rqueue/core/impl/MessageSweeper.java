/*
 * Copyright (c) 2021-2023 Sonu Kumar
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

import com.github.sonus21.rqueue.config.RqueueConfig;
import com.github.sonus21.rqueue.core.RqueueMessage;
import com.github.sonus21.rqueue.core.RqueueMessageTemplate;
import com.github.sonus21.rqueue.core.support.RqueueMessageUtils;
import com.github.sonus21.rqueue.exception.UnknownSwitchCase;
import com.github.sonus21.rqueue.listener.QueueDetail;
import com.github.sonus21.rqueue.utils.RetryableRunnable;
import com.github.sonus21.rqueue.utils.StringUtils;
import com.github.sonus21.rqueue.web.service.RqueueMessageMetadataService;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.ListUtils;
import org.springframework.data.redis.connection.DataType;
import org.springframework.util.CollectionUtils;

@Slf4j
public class MessageSweeper {

  private static MessageSweeper messageSweeper;

  private final ExecutorService executorService;
  private final RqueueMessageTemplate messageTemplate;
  private final RqueueMessageMetadataService rqueueMessageMetadataService;
  private final RqueueConfig rqueueConfig;

  private MessageSweeper(
      RqueueConfig rqueueConfig,
      RqueueMessageTemplate messageTemplate,
      RqueueMessageMetadataService rqueueMessageMetadataService) {
    this.rqueueMessageMetadataService = rqueueMessageMetadataService;
    this.executorService = Executors.newSingleThreadExecutor();
    this.messageTemplate = messageTemplate;
    this.rqueueConfig = rqueueConfig;
  }

  public static MessageSweeper getInstance(
      RqueueConfig rqueueConfig,
      RqueueMessageTemplate messageTemplate,
      RqueueMessageMetadataService rqueueMessageMetadataDao) {
    if (MessageSweeper.messageSweeper == null) {
      synchronized (MessageSweeper.class) {
        if (MessageSweeper.messageSweeper == null) {
          MessageSweeper.messageSweeper =
              new MessageSweeper(rqueueConfig, messageTemplate, rqueueMessageMetadataDao);
          return MessageSweeper.messageSweeper;
        }
        return MessageSweeper.messageSweeper;
      }
    }
    return MessageSweeper.messageSweeper;
  }

  public boolean deleteAllMessages(MessageDeleteRequest request) {
    log.debug("MessageDeleteRequest {}", request);
    if (!request.isValid()) {
      throw new IllegalArgumentException("Message request is not valid");
    }
    List<DeleteJobData> deleteJobData = new ArrayList<>();
    QueueDetail detail = request.queueDetail;
    if (detail != null) {
      String newQueueName = rqueueConfig.getDelDataName(detail.getQueueName());
      String newScheduledZsetName = rqueueConfig.getDelDataName(detail.getQueueName());
      String newProcessingZsetName = rqueueConfig.getDelDataName(detail.getQueueName());
      messageTemplate.renameCollections(
          Arrays.asList(
              detail.getQueueName(),
              detail.getScheduledQueueName(),
              detail.getProcessingQueueName()),
          Arrays.asList(newQueueName, newScheduledZsetName, newProcessingZsetName));
      deleteJobData.add(new DeleteJobData(newQueueName, DataType.LIST));
      deleteJobData.add(new DeleteJobData(newScheduledZsetName, DataType.ZSET));
      deleteJobData.add(new DeleteJobData(newProcessingZsetName, DataType.ZSET));
    } else {
      switch (request.dataType) {
        case LIST:
          DeleteJobData data =
              new DeleteJobData(rqueueConfig.getDelDataName(request.dataName), request.dataType);
          messageTemplate.renameCollection(request.dataName, data.name);
          deleteJobData.add(data);
          break;
        case ZSET:
          data = new DeleteJobData(rqueueConfig.getDelDataName(request.dataName), request.dataType);
          messageTemplate.renameCollection(request.dataName, data.name);
          deleteJobData.add(data);
          break;
        default:
          throw new UnknownSwitchCase(request.dataType.code());
      }
    }
    if (!CollectionUtils.isEmpty(deleteJobData)) {
      if (detail != null) {
        executorService.submit(new MessageDeleteJob(deleteJobData, detail.getName()));
      } else {
        executorService.submit(new MessageDeleteJob(deleteJobData, request.queueName));
      }
    }
    return true;
  }

  @AllArgsConstructor
  private static class DeleteJobData {

    private final String name;
    private final DataType type;
  }

  @Builder
  @ToString
  public static class MessageDeleteRequest {

    private final QueueDetail queueDetail;
    private final String dataName;
    private final String queueName;
    private final DataType dataType;

    private boolean isValid() {
      if (queueDetail != null) {
        return true;
      }
      return !StringUtils.isEmpty(dataName)
          && !StringUtils.isEmpty(queueName)
          && Arrays.asList(DataType.LIST, DataType.ZSET).contains(dataType);
    }
  }

  private class MessageDeleteJob extends RetryableRunnable<DeleteJobData> {

    private static final int batchSize = 1000;
    private final String queueName;

    MessageDeleteJob(List<DeleteJobData> jobData, String queueName) {
      super(log, null, jobData.iterator());
      this.queueName = queueName;
    }

    private List<String> getMessageIdFromList(String queueName) {
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

    private List<String> getMessageIds(DeleteJobData data) {
      if (data.type == DataType.LIST) {
        return getMessageIdFromList(data.name);
      }
      return getMessageIdFromZset(data.name);
    }

    public void delete(DeleteJobData data) {
      for (List<String> subIds : ListUtils.partition(getMessageIds(data), batchSize)) {
        List<String> messageMetaIds =
            subIds.stream()
                .map(e -> RqueueMessageUtils.getMessageMetaId(queueName, e))
                .collect(Collectors.toList());
        rqueueMessageMetadataService.deleteAll(messageMetaIds);
        log.debug("Deleted {} messages meta", messageMetaIds.size());
      }
      messageTemplate.deleteCollection(data.name);
    }

    @Override
    public void consume(DeleteJobData data) {
      delete(data);
    }
  }
}
