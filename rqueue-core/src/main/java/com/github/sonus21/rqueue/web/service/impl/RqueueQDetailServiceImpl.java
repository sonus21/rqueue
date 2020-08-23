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

package com.github.sonus21.rqueue.web.service.impl;

import static com.github.sonus21.rqueue.utils.StringUtils.clean;
import static com.google.common.collect.Lists.newArrayList;

import com.github.sonus21.rqueue.common.RqueueRedisTemplate;
import com.github.sonus21.rqueue.core.RqueueMessage;
import com.github.sonus21.rqueue.core.RqueueMessageTemplate;
import com.github.sonus21.rqueue.exception.UnknownSwitchCase;
import com.github.sonus21.rqueue.models.db.DeadLetterQueue;
import com.github.sonus21.rqueue.models.db.MessageMetadata;
import com.github.sonus21.rqueue.models.db.QueueConfig;
import com.github.sonus21.rqueue.models.enums.ActionType;
import com.github.sonus21.rqueue.models.enums.DataType;
import com.github.sonus21.rqueue.models.enums.NavTab;
import com.github.sonus21.rqueue.models.response.DataViewResponse;
import com.github.sonus21.rqueue.models.response.RedisDataDetail;
import com.github.sonus21.rqueue.utils.Constants;
import com.github.sonus21.rqueue.utils.DateTimeUtils;
import com.github.sonus21.rqueue.utils.MessageUtils;
import com.github.sonus21.rqueue.utils.RedisUtils;
import com.github.sonus21.rqueue.utils.StringUtils;
import com.github.sonus21.rqueue.web.service.RqueueMessageMetadataService;
import com.github.sonus21.rqueue.web.service.RqueueQDetailService;
import com.github.sonus21.rqueue.web.service.RqueueSystemManagerService;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.stream.Collectors;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.data.redis.core.DefaultTypedTuple;
import org.springframework.data.redis.core.ZSetOperations.TypedTuple;
import org.springframework.stereotype.Service;
import org.springframework.util.CollectionUtils;

@Service
public class RqueueQDetailServiceImpl implements RqueueQDetailService {
  private final RqueueRedisTemplate<String> stringRqueueRedisTemplate;
  private final RqueueMessageTemplate rqueueMessageTemplate;
  private final RqueueSystemManagerService rqueueSystemManagerService;
  private final RqueueMessageMetadataService rqueueMessageMetadataService;

  @Autowired
  public RqueueQDetailServiceImpl(
      @Qualifier("stringRqueueRedisTemplate") RqueueRedisTemplate<String> stringRqueueRedisTemplate,
      RqueueMessageTemplate rqueueMessageTemplate,
      RqueueSystemManagerService rqueueSystemManagerService,
      RqueueMessageMetadataService rqueueMessageMetadataService) {
    this.stringRqueueRedisTemplate = stringRqueueRedisTemplate;
    this.rqueueMessageTemplate = rqueueMessageTemplate;
    this.rqueueSystemManagerService = rqueueSystemManagerService;
    this.rqueueMessageMetadataService = rqueueMessageMetadataService;
  }

  @Override
  public Map<String, List<Entry<NavTab, RedisDataDetail>>> getQueueDataStructureDetails(
      List<QueueConfig> queueConfig) {
    return queueConfig
        .parallelStream()
        .collect(Collectors.toMap(QueueConfig::getName, this::getQueueDataStructureDetail));
  }

  @Override
  public List<Entry<NavTab, RedisDataDetail>> getQueueDataStructureDetail(QueueConfig queueConfig) {
    if (queueConfig == null) {
      return Collections.emptyList();
    }
    Long pending = stringRqueueRedisTemplate.getListSize(queueConfig.getQueueName());
    String processingQueueName = queueConfig.getProcessingQueueName();
    Long running = stringRqueueRedisTemplate.getZsetSize(processingQueueName);
    List<Entry<NavTab, RedisDataDetail>> queueRedisDataDetails =
        newArrayList(
            new HashMap.SimpleEntry<>(
                NavTab.PENDING,
                new RedisDataDetail(
                    queueConfig.getQueueName(), DataType.LIST, pending == null ? 0 : pending)),
            new HashMap.SimpleEntry<>(
                NavTab.RUNNING,
                new RedisDataDetail(
                    processingQueueName, DataType.ZSET, running == null ? 0 : running)));
    String timeQueueName = queueConfig.getDelayedQueueName();
    Long scheduled = stringRqueueRedisTemplate.getZsetSize(timeQueueName);
    queueRedisDataDetails.add(
        new HashMap.SimpleEntry<>(
            NavTab.SCHEDULED,
            new RedisDataDetail(timeQueueName, DataType.ZSET, scheduled == null ? 0 : scheduled)));
    if (!CollectionUtils.isEmpty(queueConfig.getDeadLetterQueues())) {
      for (DeadLetterQueue dlq : queueConfig.getDeadLetterQueues()) {
        if (!dlq.isConsumerEnabled()) {
          Long dlqSize = stringRqueueRedisTemplate.getListSize(dlq.getName());
          queueRedisDataDetails.add(
              new HashMap.SimpleEntry<>(
                  NavTab.DEAD,
                  new RedisDataDetail(
                      dlq.getName(), DataType.LIST, dlqSize == null ? 0 : dlqSize)));
        } else {
          // TODO should we redirect to the queue page?
          queueRedisDataDetails.add(
              new HashMap.SimpleEntry<>(
                  NavTab.DEAD, new RedisDataDetail(dlq.getName(), DataType.LIST, -1)));
        }
      }
    }
    return queueRedisDataDetails;
  }

  @Override
  public List<NavTab> getNavTabs(QueueConfig queueConfig) {
    List<NavTab> navTabs = new ArrayList<>();
    if (queueConfig != null) {
      navTabs.add(NavTab.PENDING);
      navTabs.add(NavTab.SCHEDULED);
      navTabs.add(NavTab.RUNNING);
      if (queueConfig.hasDeadLetterQueue()) {
        navTabs.add(NavTab.DEAD);
      }
    }
    return navTabs;
  }

  private List<TypedTuple<RqueueMessage>> readFromZset(
      String name, int pageNumber, int itemPerPage) {
    long start = pageNumber * (long) itemPerPage;
    long end = start + itemPerPage - 1;

    return rqueueMessageTemplate.readFromZset(name, start, end).stream()
        .map(e -> new DefaultTypedTuple<>(e, null))
        .collect(Collectors.toList());
  }

  private List<TypedTuple<RqueueMessage>> readFromList(
      String name, int pageNumber, int itemPerPage) {
    long start = pageNumber * (long) itemPerPage;
    long end = start + itemPerPage - 1;
    return rqueueMessageTemplate.readFromList(name, start, end).stream()
        .map(e -> new DefaultTypedTuple<>(e, null))
        .collect(Collectors.toList());
  }

  private List<TypedTuple<RqueueMessage>> readFromZetWithScore(
      String name, int pageNumber, int itemPerPage) {
    long start = pageNumber * (long) itemPerPage;
    long end = start + itemPerPage - 1;
    return rqueueMessageTemplate.readFromZsetWithScore(name, start, end);
  }

  private List<List<Serializable>> buildRows(
      List<TypedTuple<RqueueMessage>> rqueueMessages, RowBuilder rowBuilder) {
    if (CollectionUtils.isEmpty(rqueueMessages)) {
      return Collections.emptyList();
    }
    Map<String, String> messageMetaIdToId = new HashMap<>();
    for (TypedTuple<RqueueMessage> tuple : rqueueMessages) {
      RqueueMessage rqueueMessage = tuple.getValue();
      assert rqueueMessage != null;
      String messageMetaId =
          MessageUtils.getMessageMetaId(rqueueMessage.getQueueName(), rqueueMessage.getId());
      messageMetaIdToId.put(messageMetaId, rqueueMessage.getId());
    }
    List<MessageMetadata> vals = rqueueMessageMetadataService.findAll(messageMetaIdToId.keySet());
    Map<String, Boolean> msgIdToDeleted = new HashMap<>();
    for (MessageMetadata messageMetadata : vals) {
      String messageMetaId = messageMetadata.getId();
      String id = messageMetaIdToId.get(messageMetaId);
      msgIdToDeleted.put(id, messageMetadata.isDeleted());
    }
    return rqueueMessages.stream()
        .map(
            e ->
                rowBuilder.row(
                    e.getValue(),
                    msgIdToDeleted.getOrDefault(e.getValue().getId(), false),
                    e.getScore()))
        .collect(Collectors.toList());
  }

  @Override
  public DataViewResponse getExplorePageData(
      String src, String name, DataType type, int pageNumber, int itemPerPage) {
    QueueConfig queueConfig = rqueueSystemManagerService.getQueueConfig(src);
    DataViewResponse response = new DataViewResponse();
    boolean deadLetterQueue = queueConfig.isDeadLetterQueue(name);
    boolean timeQueue = queueConfig.getDelayedQueueName().equals(name);
    setHeadersIfRequired(deadLetterQueue, type, response, pageNumber);
    if (deadLetterQueue) {
      response.addAction(ActionType.DELETE);
    } else {
      response.addAction(ActionType.NONE);
    }
    switch (type) {
      case ZSET:
        if (timeQueue) {
          response.setRows(
              buildRows(readFromZset(name, pageNumber, itemPerPage), new ZsetRowBuilder(true)));
        } else {
          response.setRows(
              buildRows(
                  readFromZetWithScore(name, pageNumber, itemPerPage), new ZsetRowBuilder(false)));
        }
        break;
      case LIST:
        response.setRows(
            buildRows(
                readFromList(name, pageNumber, itemPerPage), new ListRowBuilder(deadLetterQueue)));
        break;
      default:
        throw new UnknownSwitchCase(type.name());
    }
    return response;
  }

  private DataViewResponse responseForSet(String name) {
    List<Object> items = new ArrayList<>(stringRqueueRedisTemplate.getMembers(name));
    DataViewResponse response = new DataViewResponse();
    response.setHeaders(Collections.singletonList("Item"));
    List<List<Serializable>> rows = new ArrayList<>();
    for (Object item : items) {
      rows.add(Collections.singletonList(item.toString()));
    }
    response.setRows(rows);
    return response;
  }

  private DataViewResponse responseForKeyVal(String name) {
    DataViewResponse response = new DataViewResponse();
    response.setHeaders(Collections.singletonList("Value"));
    Object val = stringRqueueRedisTemplate.get(name);
    List<List<Serializable>> rows =
        Collections.singletonList(Collections.singletonList(String.valueOf(val)));
    response.setRows(rows);
    return response;
  }

  private DataViewResponse responseForZset(
      String name, String key, int pageNumber, int itemPerPage) {
    DataViewResponse response = new DataViewResponse();
    int start = pageNumber * itemPerPage;
    int end = start + itemPerPage - 1;
    List<List<Serializable>> rows = new ArrayList<>();
    if (!StringUtils.isEmpty(key)) {
      Double score = stringRqueueRedisTemplate.getZsetMemberScore(name, key);
      response.setHeaders(Collections.singletonList("Score"));
      rows.add(Collections.singletonList(score));
    } else {
      response.setHeaders(Arrays.asList("Item", "Score"));
      for (TypedTuple<String> tuple : stringRqueueRedisTemplate.zrangeWithScore(name, start, end)) {
        rows.add(Arrays.asList(String.valueOf(tuple.getValue()), tuple.getScore()));
      }
    }
    response.setRows(rows);
    return response;
  }

  private DataViewResponse responseForList(String name, int pageNumber, int itemPerPage) {
    DataViewResponse response = new DataViewResponse();
    response.setHeaders(Collections.singletonList("Item"));
    int start = pageNumber * itemPerPage;
    int end = start + itemPerPage - 1;
    List<List<Serializable>> rows = new ArrayList<>();
    for (Object s : stringRqueueRedisTemplate.lrange(name, start, end)) {
      List<Serializable> singletonList = Collections.singletonList(String.valueOf(s));
      rows.add(singletonList);
    }
    response.setRows(rows);
    return response;
  }

  @Override
  public DataViewResponse viewData(
      String name, DataType type, String key, int pageNumber, int itemPerPage) {
    if (StringUtils.isEmpty(name)) {
      return DataViewResponse.createErrorMessage("Data name cannot be empty.");
    }
    if (DataType.isUnknown(type)) {
      return DataViewResponse.createErrorMessage("Data type is not provided.");
    }
    switch (type) {
      case SET:
        return responseForSet(clean(name));
      case ZSET:
        return responseForZset(clean(name), clean(key), pageNumber, itemPerPage);
      case LIST:
        return responseForList(clean(name), pageNumber, itemPerPage);
      case KEY:
        return responseForKeyVal(clean(name));
      default:
        throw new UnknownSwitchCase(type.name());
    }
  }

  private void setHeadersIfRequired(
      boolean deadLetterQueue, DataType type, DataViewResponse response, int pageNumber) {
    if (pageNumber != 0) {
      return;
    }
    List<String> headers = newArrayList("Id", "Message");
    if (DataType.ZSET == type) {
      headers.add("Time Left");
    }
    if (!deadLetterQueue) {
      headers.add("Action");
    } else {
      headers.add("AddedOn");
    }
    response.setHeaders(headers);
  }

  @Override
  public List<List<Object>> getRunningTasks() {
    List<QueueConfig> queueConfigs = rqueueSystemManagerService.getSortedQueueConfigs();
    List<List<Object>> rows = new ArrayList<>();
    List<Object> result = new ArrayList<>();
    if (!CollectionUtils.isEmpty(queueConfigs)) {
      result =
          RedisUtils.executePipeLine(
              stringRqueueRedisTemplate.getRedisTemplate(),
              ((connection, keySerializer, valueSerializer) -> {
                for (QueueConfig queueConfig : queueConfigs) {
                  connection.zCard(queueConfig.getProcessingQueueName().getBytes());
                }
              }));
    }
    rows.add(Arrays.asList("Queue", "Processing [ZSET]", "Size"));
    for (int i = 0; i < queueConfigs.size(); i++) {
      QueueConfig queueConfig = queueConfigs.get(i);
      rows.add(
          Arrays.asList(
              queueConfig.getName(), queueConfig.getProcessingQueueName(), result.get(i)));
    }
    return rows;
  }

  @Override
  public List<List<Object>> getWaitingTasks() {
    List<QueueConfig> queueConfigs = rqueueSystemManagerService.getSortedQueueConfigs();
    List<List<Object>> rows = new ArrayList<>();
    List<Object> result = new ArrayList<>();
    if (!CollectionUtils.isEmpty(queueConfigs)) {
      result =
          RedisUtils.executePipeLine(
              stringRqueueRedisTemplate.getRedisTemplate(),
              ((connection, keySerializer, valueSerializer) -> {
                for (QueueConfig queueConfig : queueConfigs) {
                  connection.lLen(queueConfig.getQueueName().getBytes());
                }
              }));
    }
    rows.add(Arrays.asList("Queue", "Queue [LIST]", "Size"));
    for (int i = 0; i < queueConfigs.size(); i++) {
      QueueConfig queueConfig = queueConfigs.get(i);
      rows.add(Arrays.asList(queueConfig.getName(), queueConfig.getQueueName(), result.get(i)));
    }
    return rows;
  }

  @Override
  public List<List<Object>> getScheduledTasks() {
    List<QueueConfig> queueConfigs = rqueueSystemManagerService.getSortedQueueConfigs();
    List<List<Object>> rows = new ArrayList<>();
    List<Object> result = new ArrayList<>();
    if (!CollectionUtils.isEmpty(queueConfigs)) {
      result =
          RedisUtils.executePipeLine(
              stringRqueueRedisTemplate.getRedisTemplate(),
              ((connection, keySerializer, valueSerializer) -> {
                for (QueueConfig queueConfig : queueConfigs) {
                  connection.zCard(queueConfig.getDelayedQueueName().getBytes());
                }
              }));
    }
    rows.add(Arrays.asList("Queue", "Scheduled [ZSET]", "Size"));
    for (int i = 0; i < queueConfigs.size(); i++) {
      QueueConfig queueConfig = queueConfigs.get(i);
      rows.add(
          Arrays.asList(queueConfig.getName(), queueConfig.getDelayedQueueName(), result.get(i)));
    }
    return rows;
  }

  private void addRows(
      List<Object> result,
      List<List<Object>> rows,
      List<Entry<QueueConfig, String>> queueConfigAndDlq) {
    for (int i = 0, j = 0; i < queueConfigAndDlq.size(); i++) {
      Entry<QueueConfig, String> entry = queueConfigAndDlq.get(i);
      QueueConfig queueConfig = entry.getKey();
      if (entry.getValue().isEmpty()) {
        rows.add(Arrays.asList(queueConfig.getName(), "", ""));
      } else {
        String name = "";
        if (i == 0
            || !queueConfig
                .getQueueName()
                .equals(queueConfigAndDlq.get(i - 1).getKey().getQueueName())) {
          name = queueConfig.getName();
        }
        rows.add(Arrays.asList(name, entry.getValue(), result.get(j++)));
      }
    }
  }

  @Override
  public List<List<Object>> getDeadLetterTasks() {
    List<QueueConfig> queueConfigs = rqueueSystemManagerService.getSortedQueueConfigs();
    List<Entry<QueueConfig, String>> queueConfigAndDlq = new ArrayList<>();
    for (QueueConfig queueConfig : queueConfigs) {
      if (queueConfig.hasDeadLetterQueue()) {
        for (DeadLetterQueue dlq : queueConfig.getDeadLetterQueues()) {
          queueConfigAndDlq.add(new HashMap.SimpleEntry<>(queueConfig, dlq.getName()));
        }
      } else {
        queueConfigAndDlq.add(new HashMap.SimpleEntry<>(queueConfig, ""));
      }
    }
    List<List<Object>> rows = new ArrayList<>();
    List<Object> result = new ArrayList<>();
    if (!CollectionUtils.isEmpty(queueConfigAndDlq)) {
      result =
          RedisUtils.executePipeLine(
              stringRqueueRedisTemplate.getRedisTemplate(),
              ((connection, keySerializer, valueSerializer) -> {
                for (Entry<QueueConfig, String> entry : queueConfigAndDlq) {
                  if (!entry.getValue().isEmpty()) {
                    connection.lLen(entry.getValue().getBytes());
                  }
                }
              }));
    }
    rows.add(Arrays.asList("Queue", "Dead Letter Queue [LIST]", "Size"));
    addRows(result, rows, queueConfigAndDlq);
    return rows;
  }

  private interface RowBuilder {
    List<Serializable> row(RqueueMessage rqueueMessage, boolean deleted, Double score);
  }

  static class ListRowBuilder implements RowBuilder {
    private final boolean deadLetterQueue;

    ListRowBuilder(boolean deadLetterQueue) {
      this.deadLetterQueue = deadLetterQueue;
    }

    @Override
    public List<Serializable> row(RqueueMessage rqueueMessage, boolean deleted, Double score) {
      List<Serializable> row = newArrayList(rqueueMessage.getId(), rqueueMessage.toString());
      if (!deadLetterQueue) {
        if (deleted) {
          row.add("");
        } else {
          row.add(ActionType.DELETE);
        }
      } else {
        row.add(DateTimeUtils.formatMilliToString(rqueueMessage.getReEnqueuedAt()));
      }
      return row;
    }
  }

  class ZsetRowBuilder implements RowBuilder {
    private final long currentTime;
    private final boolean timeQueue;

    ZsetRowBuilder(boolean timeQueue) {
      this.timeQueue = timeQueue;
      this.currentTime = System.currentTimeMillis();
    }

    @Override
    public List<Serializable> row(RqueueMessage rqueueMessage, boolean deleted, Double score) {
      List<Serializable> row = newArrayList(rqueueMessage.getId(), rqueueMessage.toString());
      if (timeQueue) {
        row.add(
            DateTimeUtils.milliToHumanRepresentation(rqueueMessage.getProcessAt() - currentTime));
      } else {
        row.add(DateTimeUtils.milliToHumanRepresentation(score.longValue() - currentTime));
      }
      if (!deleted) {
        row.add(ActionType.DELETE);
      } else {
        row.add(Constants.BLANK);
      }
      return row;
    }
  }
}
