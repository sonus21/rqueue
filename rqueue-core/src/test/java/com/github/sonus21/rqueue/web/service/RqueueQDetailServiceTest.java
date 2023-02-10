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

package com.github.sonus21.rqueue.web.service;

import static com.github.sonus21.rqueue.utils.TestUtils.createQueueConfig;
import static com.google.common.collect.Lists.newArrayList;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyCollection;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;

import com.github.sonus21.TestBase;
import com.github.sonus21.rqueue.CoreUnitTest;
import com.github.sonus21.rqueue.common.RqueueRedisTemplate;
import com.github.sonus21.rqueue.config.RqueueConfig;
import com.github.sonus21.rqueue.converter.GenericMessageConverter;
import com.github.sonus21.rqueue.core.RqueueMessage;
import com.github.sonus21.rqueue.core.RqueueMessageTemplate;
import com.github.sonus21.rqueue.core.support.RqueueMessageUtils;
import com.github.sonus21.rqueue.models.db.DeadLetterQueue;
import com.github.sonus21.rqueue.models.db.MessageMetadata;
import com.github.sonus21.rqueue.models.db.QueueConfig;
import com.github.sonus21.rqueue.models.enums.ActionType;
import com.github.sonus21.rqueue.models.enums.DataType;
import com.github.sonus21.rqueue.models.enums.MessageStatus;
import com.github.sonus21.rqueue.models.enums.NavTab;
import com.github.sonus21.rqueue.models.enums.TableColumnType;
import com.github.sonus21.rqueue.models.response.Action;
import com.github.sonus21.rqueue.models.response.DataViewResponse;
import com.github.sonus21.rqueue.models.response.RedisDataDetail;
import com.github.sonus21.rqueue.models.response.RowColumnMeta;
import com.github.sonus21.rqueue.models.response.RowColumnMetaType;
import com.github.sonus21.rqueue.models.response.TableColumn;
import com.github.sonus21.rqueue.models.response.TableRow;
import com.github.sonus21.rqueue.web.service.impl.RqueueQDetailServiceImpl;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.stream.Collectors;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.springframework.data.redis.core.DefaultTypedTuple;
import org.springframework.data.redis.core.RedisCallback;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.ZSetOperations.TypedTuple;
import org.springframework.messaging.converter.MessageConverter;

@CoreUnitTest
class RqueueQDetailServiceTest extends TestBase {

  private final MessageConverter messageConverter = new GenericMessageConverter();
  @Mock
  private RedisTemplate<?, ?> redisTemplate;
  @Mock
  private RqueueRedisTemplate<String> stringRqueueRedisTemplate;
  @Mock
  private RqueueMessageTemplate rqueueMessageTemplate;
  @Mock
  private RqueueSystemManagerService rqueueSystemManagerService;
  @Mock
  private RqueueMessageMetadataService rqueueMessageMetadataService;
  private RqueueQDetailService rqueueQDetailService;
  private QueueConfig queueConfig;
  private QueueConfig queueConfig2;
  private List<QueueConfig> queueConfigList;
  private Collection<String> queues;
  private RqueueConfig rqueueConfig = mock(RqueueConfig.class);

  @BeforeEach
  public void init() {
    MockitoAnnotations.openMocks(this);
    rqueueQDetailService =
        new RqueueQDetailServiceImpl(
            stringRqueueRedisTemplate,
            rqueueMessageTemplate,
            rqueueSystemManagerService,
            rqueueMessageMetadataService,
            rqueueConfig);
    queueConfig = createQueueConfig("test", 10, 10000L, "test-dlq");
    queueConfig2 = createQueueConfig("test2", 10, 10000L, null);
    queueConfigList = Arrays.asList(queueConfig, queueConfig2);
    queues = Arrays.asList(queueConfig.getName(), queueConfig2.getName());
  }

  @Test
  void getQueueDataStructureDetail() {
    assertEquals(Collections.emptyList(), rqueueQDetailService.getQueueDataStructureDetail(null));
    doReturn(10L).when(stringRqueueRedisTemplate).getListSize("__rq::queue::test");
    doReturn(11L).when(stringRqueueRedisTemplate).getListSize("test-dlq");
    doReturn(12L).when(stringRqueueRedisTemplate).getZsetSize("__rq::d-queue::test");
    doReturn(5L).when(stringRqueueRedisTemplate).getZsetSize("__rq::p-queue::test");
    List<Entry<NavTab, RedisDataDetail>> queueRedisDataDetails = new ArrayList<>();
    queueRedisDataDetails.add(
        new HashMap.SimpleEntry<>(
            NavTab.PENDING, new RedisDataDetail("__rq::queue::test", DataType.LIST, 10)));
    queueRedisDataDetails.add(
        new HashMap.SimpleEntry<>(
            NavTab.RUNNING, new RedisDataDetail("__rq::p-queue::test", DataType.ZSET, 5)));
    queueRedisDataDetails.add(
        new HashMap.SimpleEntry<>(
            NavTab.SCHEDULED, new RedisDataDetail("__rq::d-queue::test", DataType.ZSET, 12)));
    queueRedisDataDetails.add(
        new HashMap.SimpleEntry<>(
            NavTab.DEAD,
            new RedisDataDetail(
                queueConfig.getDeadLetterQueues().stream().findFirst().get().getName(),
                DataType.LIST,
                11)));
    assertEquals(
        queueRedisDataDetails, rqueueQDetailService.getQueueDataStructureDetail(queueConfig));
  }

  @Test
  void getQueueDataStructureDetails() {
    doReturn(10L).when(stringRqueueRedisTemplate).getListSize("__rq::queue::test");
    doReturn(11L).when(stringRqueueRedisTemplate).getListSize("test-dlq");
    doReturn(12L).when(stringRqueueRedisTemplate).getZsetSize("__rq::d-queue::test");
    doReturn(5L).when(stringRqueueRedisTemplate).getZsetSize("__rq::p-queue::test");
    List<Entry<NavTab, RedisDataDetail>> queueRedisDataDetails = new ArrayList<>();
    queueRedisDataDetails.add(
        new HashMap.SimpleEntry<>(
            NavTab.PENDING, new RedisDataDetail("__rq::queue::test", DataType.LIST, 10)));
    queueRedisDataDetails.add(
        new HashMap.SimpleEntry<>(
            NavTab.RUNNING, new RedisDataDetail("__rq::p-queue::test", DataType.ZSET, 5)));
    queueRedisDataDetails.add(
        new HashMap.SimpleEntry<>(
            NavTab.SCHEDULED, new RedisDataDetail("__rq::d-queue::test", DataType.ZSET, 12)));
    queueRedisDataDetails.add(
        new HashMap.SimpleEntry<>(
            NavTab.DEAD,
            new RedisDataDetail(
                queueConfig.getDeadLetterQueues().stream().findFirst().get().getName(),
                DataType.LIST,
                11)));

    doReturn(5L).when(stringRqueueRedisTemplate).getListSize("__rq::queue::test2");
    doReturn(2L).when(stringRqueueRedisTemplate).getZsetSize("__rq::p-queue::test2");
    doReturn(8L).when(stringRqueueRedisTemplate).getZsetSize("__rq::d-queue::test2");

    List<Entry<NavTab, RedisDataDetail>> queueRedisDataDetails2 = new ArrayList<>();
    queueRedisDataDetails2.add(
        new HashMap.SimpleEntry<>(
            NavTab.PENDING, new RedisDataDetail("__rq::queue::test2", DataType.LIST, 5)));
    queueRedisDataDetails2.add(
        new HashMap.SimpleEntry<>(
            NavTab.RUNNING, new RedisDataDetail("__rq::p-queue::test2", DataType.ZSET, 2)));
    queueRedisDataDetails2.add(
        new HashMap.SimpleEntry<>(
            NavTab.SCHEDULED, new RedisDataDetail("__rq::d-queue::test2", DataType.ZSET, 8)));

    Map<String, List<Entry<NavTab, RedisDataDetail>>> map = new HashMap<>();
    map.put("test", queueRedisDataDetails);
    map.put("test2", queueRedisDataDetails2);
    assertEquals(map, rqueueQDetailService.getQueueDataStructureDetails(queueConfigList));
  }

  @Test
  void getNavTabs() {
    assertEquals(Collections.emptyList(), rqueueQDetailService.getNavTabs(null));
    List<NavTab> navTabs = new ArrayList<>();
    navTabs.add(NavTab.PENDING);
    navTabs.add(NavTab.SCHEDULED);
    navTabs.add(NavTab.RUNNING);
    navTabs.add(NavTab.DEAD);
    assertEquals(navTabs, rqueueQDetailService.getNavTabs(queueConfig));
  }

  @Test
  void getExplorePageDataQueue() {
    List<RqueueMessage> rqueueMessages =
        RqueueMessageUtils.generateMessages(messageConverter, "test", 10);
    DataViewResponse expectedResponse = new DataViewResponse();
    List<String> headers = new ArrayList<>();
    headers.add("Id");
    headers.add("Message");
    headers.add("Type");
    headers.add("Action");
    expectedResponse.setHeaders(headers);
    List<TableRow> lists = new ArrayList<>();
    for (RqueueMessage message : rqueueMessages) {
      List<TableColumn> l = new ArrayList<>();
      l.add(new TableColumn(message.getId()));
      l.add(
          new TableColumn(
              TableColumnType.DISPLAY,
              message.toString(),
              Collections.singletonList(
                  new RowColumnMeta(RowColumnMetaType.JOBS_BUTTON, message.getId()))));

      l.add(new TableColumn("Simple"));
      l.add(new TableColumn(TableColumnType.ACTION, ActionType.DELETE));
      lists.add(new TableRow(l));
    }
    expectedResponse.setRows(lists);
    expectedResponse.addAction(new Action(ActionType.DELETE, "pending messages for queue 'test'"));
    doReturn(queueConfig).when(rqueueSystemManagerService).getQueueConfig("test");
    doReturn(rqueueMessages).when(rqueueMessageTemplate).readFromList("test", 0, 9);
    DataViewResponse response =
        rqueueQDetailService.getExplorePageData("test", "test", DataType.LIST, 0, 10);
    assertEquals(expectedResponse, response);
  }

  @Test
  void getExplorePageDataDeadLetterQueue() {
    List<RqueueMessage> rqueueMessages =
        RqueueMessageUtils.generateMessages(messageConverter, "test", 10);
    DataViewResponse expectedResponse = new DataViewResponse();
    List<String> headers = new ArrayList<>();
    headers.add("Id");
    headers.add("Message");
    headers.add("Type");
    headers.add("AddedOn");
    expectedResponse.setHeaders(headers);
    List<TableRow> lists = new ArrayList<>();
    for (RqueueMessage message : rqueueMessages) {
      message.setReEnqueuedAt(System.currentTimeMillis());
      List<TableColumn> l = new ArrayList<>();
      l.add(new TableColumn(message.getId()));
      l.add(
          new TableColumn(
              TableColumnType.DISPLAY,
              message.toString(),
              Collections.singletonList(
                  new RowColumnMeta(RowColumnMetaType.JOBS_BUTTON, message.getId()))));

      l.add(new TableColumn("Simple"));
      lists.add(new TableRow(l));
    }
    expectedResponse.setRows(lists);
    expectedResponse.addAction(new Action(ActionType.DELETE, "dead letter queue 'test-dlq'"));
    doReturn(queueConfig).when(rqueueSystemManagerService).getQueueConfig("test");
    doReturn(rqueueMessages).when(rqueueMessageTemplate).readFromList("test-dlq", 0, 9);
    DataViewResponse response =
        rqueueQDetailService.getExplorePageData("test", "test-dlq", DataType.LIST, 0, 10);
    for (TableRow row : response.getRows()) {
      assertNotEquals("", row.getColumns().get(3).getValue());
      row.getColumns().remove(3);
    }
    assertEquals(expectedResponse, response);
  }

  @Test
  void getExplorePageDataTypeQueueDeleteFewItems() {
    QueueConfig queueConfig = createQueueConfig("test", 10, 10000L, null);
    queueConfig.addDeadLetterQueue(new DeadLetterQueue("test-dlq", false));
    List<RqueueMessage> rqueueMessages =
        RqueueMessageUtils.generateMessages(messageConverter, "test", 10);
    List<MessageMetadata> messageMetadata = new ArrayList<>();
    for (int i = 0; i < 5; i++) {
      RqueueMessage message = rqueueMessages.get(i);
      MessageMetadata metadata = new MessageMetadata(message, MessageStatus.DELETED);
      metadata.setDeleted(true);
      messageMetadata.add(metadata);
    }
    DataViewResponse expectedResponse = new DataViewResponse();
    List<String> headers = new ArrayList<>();
    headers.add("Id");
    headers.add("Message");
    headers.add("Type");
    headers.add("Action");
    expectedResponse.setHeaders(headers);
    List<TableRow> lists = new ArrayList<>();
    for (int i = 0; i < 10; i++) {
      RqueueMessage message = rqueueMessages.get(i);
      List<TableColumn> l = new ArrayList<>();
      l.add(new TableColumn(message.getId()));
      l.add(
          new TableColumn(
              TableColumnType.DISPLAY,
              message.toString(),
              Collections.singletonList(
                  new RowColumnMeta(RowColumnMetaType.JOBS_BUTTON, message.getId()))));
      l.add(new TableColumn("Simple"));
      if (i >= 5) {
        l.add(new TableColumn(TableColumnType.ACTION, ActionType.DELETE));
      } else {
        l.add(new TableColumn(""));
      }
      lists.add(new TableRow(l));
    }
    expectedResponse.setRows(lists);
    expectedResponse.addAction(new Action(ActionType.DELETE, "pending messages for queue 'test'"));

    doReturn(rqueueMessages).when(rqueueMessageTemplate).readFromList("test", 0, 9);
    doReturn(messageMetadata).when(rqueueMessageMetadataService).findAll(anyCollection());
    doReturn(queueConfig).when(rqueueSystemManagerService).getQueueConfig("test");
    DataViewResponse response =
        rqueueQDetailService.getExplorePageData("test", "test", DataType.LIST, 0, 10);
    assertEquals(expectedResponse, response);
  }

  @Test
  void getExplorePageDataTypeScheduledQueue() {
    QueueConfig queueConfig = createQueueConfig("test", 10, 10000L, null);
    queueConfig.addDeadLetterQueue(new DeadLetterQueue("test-dlq", false));
    List<RqueueMessage> rqueueMessages =
        RqueueMessageUtils.generateMessages(messageConverter, "test", 100000, 10);
    DataViewResponse expectedResponse = new DataViewResponse();
    List<String> headers = new ArrayList<>();
    headers.add("Id");
    headers.add("Message");
    headers.add("Type");
    headers.add("Time Left");
    headers.add("Action");
    expectedResponse.setHeaders(headers);
    List<TableRow> lists = new ArrayList<>();
    for (RqueueMessage message : rqueueMessages) {
      List<TableColumn> l = new ArrayList<>();
      l.add(new TableColumn(message.getId()));
      l.add(
          new TableColumn(
              TableColumnType.DISPLAY,
              message.toString(),
              Collections.singletonList(
                  new RowColumnMeta(RowColumnMetaType.JOBS_BUTTON, message.getId()))));
      l.add(new TableColumn("Simple"));
      l.add(new TableColumn(TableColumnType.ACTION, ActionType.DELETE));
      lists.add(new TableRow(l));
    }
    expectedResponse.setRows(lists);
    expectedResponse.addAction(
        new Action(ActionType.DELETE, "scheduled messages for queue 'test'"));

    doReturn(queueConfig).when(rqueueSystemManagerService).getQueueConfig("test");
    doReturn(rqueueMessages).when(rqueueMessageTemplate).readFromZset("__rq::d-queue::test", 0, 9);
    DataViewResponse response =
        rqueueQDetailService.getExplorePageData(
            "test", "__rq::d-queue::test", DataType.ZSET, 0, 10);
    // clear time left
    for (TableRow tableRow : response.getRows()) {
      tableRow.getColumns().remove(3);
    }
    assertEquals(expectedResponse, response);
  }

  @Test
  void getExplorePageDataTypeProcessingQueue() {
    QueueConfig queueConfig = createQueueConfig("test", 10, 10000L, null);
    queueConfig.addDeadLetterQueue(new DeadLetterQueue("test-dlq", false));
    List<RqueueMessage> rqueueMessages =
        RqueueMessageUtils.generateMessages(messageConverter, "test", 100000, 10);
    DataViewResponse expectedResponse = new DataViewResponse();
    List<String> headers = new ArrayList<>();
    headers.add("Id");
    headers.add("Message");
    headers.add("Type");
    headers.add("Time Left");
    headers.add("Action");
    expectedResponse.setHeaders(headers);
    List<TableRow> lists = new ArrayList<>();
    for (RqueueMessage message : rqueueMessages) {
      List<TableColumn> l = new ArrayList<>();
      l.add(new TableColumn(message.getId()));
      l.add(
          new TableColumn(
              TableColumnType.DISPLAY,
              message.toString(),
              Collections.singletonList(
                  new RowColumnMeta(RowColumnMetaType.JOBS_BUTTON, message.getId()))));

      l.add(new TableColumn("Simple"));
      l.add(new TableColumn(TableColumnType.ACTION, ActionType.DELETE));
      lists.add(new TableRow(l));
    }
    expectedResponse.setRows(lists);
    doReturn(queueConfig).when(rqueueSystemManagerService).getQueueConfig("test");
    doReturn(
        rqueueMessages.stream()
            .map(e -> new DefaultTypedTuple<>(e, (double) System.currentTimeMillis() + 100L))
            .collect(Collectors.toList()))
        .when(rqueueMessageTemplate)
        .readFromZsetWithScore("__rq::p-queue::test", 0, 9);

    DataViewResponse response =
        rqueueQDetailService.getExplorePageData(
            "test", "__rq::p-queue::test", DataType.ZSET, 0, 10);
    // clear time left
    for (TableRow tableRow : response.getRows()) {
      tableRow.getColumns().remove(3);
    }
    assertEquals(expectedResponse, response);
  }

  @Test
  void viewDataKey() {
    doReturn("test").when(stringRqueueRedisTemplate).get("key");
    DataViewResponse response = rqueueQDetailService.viewData("key", DataType.KEY, null, 0, 10);
    DataViewResponse expectedResponse = new DataViewResponse();
    expectedResponse.setHeaders(Collections.singletonList("Value"));
    expectedResponse.setRows(Collections.singletonList(new TableRow(new TableColumn("test"))));
    assertEquals(expectedResponse, response);

    doReturn(null).when(stringRqueueRedisTemplate).get("key2");
    response = rqueueQDetailService.viewData("key2", DataType.KEY, null, 0, 10);
    expectedResponse.setRows(Collections.singletonList(new TableRow(new TableColumn("null"))));
    assertEquals(expectedResponse, response);
  }

  @Test
  void viewDataList() {
    List<Object> objects = new ArrayList<>();
    objects.add("Test");
    objects.add(
        RqueueMessageUtils.buildMessage(
            messageConverter, "jobs", null, "buildMessage", null, null, null));
    objects.add(null);
    doReturn(objects).when(stringRqueueRedisTemplate).lrange("jobs", 0, 9);
    DataViewResponse response = rqueueQDetailService.viewData("jobs", DataType.LIST, null, 0, 10);
    DataViewResponse expectedResponse = new DataViewResponse();
    expectedResponse.setHeaders(Collections.singletonList("Item"));
    List<TableRow> tableRows = new ArrayList<>();
    for (Object o : objects) {
      tableRows.add(new TableRow(new TableColumn(String.valueOf(o))));
    }
    expectedResponse.setRows(tableRows);
    assertEquals(expectedResponse, response);
  }

  @Test
  void viewDataZset() {
    Set<TypedTuple<Object>> objects = new HashSet<>();
    objects.add(new DefaultTypedTuple<>("Test", 100.0));
    objects.add(
        new DefaultTypedTuple<>(
            RqueueMessageUtils.buildMessage(
                messageConverter, "jobs", null, "buildMessage", null, null, null),
            200.0));

    List<TableRow> tableRows = new ArrayList<>();
    for (TypedTuple<Object> typedTuple : objects) {
      List<TableColumn> items = new ArrayList<>();
      items.add(new TableColumn(String.valueOf(typedTuple.getValue())));
      items.add(new TableColumn(typedTuple.getScore()));
      tableRows.add(new TableRow(items));
    }
    DataViewResponse expectedResponse = new DataViewResponse();
    List<String> headers = new ArrayList<>();
    headers.add("Value");
    headers.add("Score");
    expectedResponse.setHeaders(headers);

    expectedResponse.setRows(tableRows);

    doReturn(objects).when(stringRqueueRedisTemplate).zrangeWithScore("jobs", 0, 9);
    DataViewResponse response = rqueueQDetailService.viewData("jobs", DataType.ZSET, null, 0, 10);

    assertEquals(expectedResponse, response);
  }

  @Test
  void viewDataSet() {
    Set<Object> objects = new HashSet<>();
    objects.add("Test");
    objects.add(
        RqueueMessageUtils.buildMessage(
            messageConverter, "jobs", null, "Test object", null, null, null));
    List<TableRow> tableRows = new ArrayList<>();
    for (Object object : objects) {
      tableRows.add(new TableRow(new TableColumn(String.valueOf(object))));
    }
    DataViewResponse expectedResponse = new DataViewResponse();
    expectedResponse.setHeaders(Collections.singletonList("Item"));
    expectedResponse.setRows(tableRows);
    doReturn(objects).when(stringRqueueRedisTemplate).getMembers("jobs");
    DataViewResponse response = rqueueQDetailService.viewData("jobs", DataType.SET, null, 0, 10);
    assertEquals(expectedResponse, response);
  }

  @Test
  void viewData() {
    DataViewResponse dataViewResponse = rqueueQDetailService.viewData(null, null, null, 0, 10);
    assertEquals("Data name cannot be empty.", dataViewResponse.getMessage());
    assertEquals(1, dataViewResponse.getCode());

    dataViewResponse = rqueueQDetailService.viewData("Test", null, null, 0, 10);
    assertEquals("Data type is not provided.", dataViewResponse.getMessage());
    assertEquals(1, dataViewResponse.getCode());
  }

  @Test
  void getScheduledTasks() {
    doReturn(redisTemplate).when(stringRqueueRedisTemplate).getRedisTemplate();
    QueueConfig queueConfig = createQueueConfig("test", 10, 10000L, null);
    queueConfig.addDeadLetterQueue(new DeadLetterQueue("test-dlq", false));
    QueueConfig queueConfig2 = createQueueConfig("test2", 10, 10000L, null);
    queueConfig.addDeadLetterQueue(new DeadLetterQueue("test-dlq-2", false));

    doReturn(Arrays.asList(queueConfig, queueConfig2))
        .when(rqueueSystemManagerService)
        .getSortedQueueConfigs();

    doReturn(newArrayList(100L, 200L))
        .when(redisTemplate)
        .executePipelined(any(RedisCallback.class));
    List<List<Object>> response = rqueueQDetailService.getScheduledTasks();
    assertEquals(3, response.size());
    List<List<Object>> expectedResponse = new ArrayList<>();
    List<Object> headers = Arrays.asList("Queue", "Scheduled [ZSET]", "Size");
    expectedResponse.add(headers);
    expectedResponse.add(
        Arrays.asList(queueConfig.getName(), queueConfig.getScheduledQueueName(), 100L));
    expectedResponse.add(
        Arrays.asList(queueConfig2.getName(), queueConfig2.getScheduledQueueName(), 200L));
    assertEquals(expectedResponse, response);
  }

  @Test
  void getWaitingTasks() {
    doReturn(redisTemplate).when(stringRqueueRedisTemplate).getRedisTemplate();
    doReturn(queueConfigList).when(rqueueSystemManagerService).getSortedQueueConfigs();
    doReturn(Arrays.asList(100L, 110L))
        .when(redisTemplate)
        .executePipelined(any(RedisCallback.class));
    List<List<Object>> response = rqueueQDetailService.getWaitingTasks();
    assertEquals(3, response.size());
    List<Object> headers = Arrays.asList("Queue", "Queue [LIST]", "Size");
    List<Object> row = Arrays.asList(queueConfig.getName(), queueConfig.getQueueName(), 100L);
    List<Object> row2 = Arrays.asList(queueConfig2.getName(), queueConfig2.getQueueName(), 110L);
    assertEquals(Arrays.asList(headers, row, row2), response);
  }

  @Test
  void getRunningTasks() {
    doReturn(redisTemplate).when(stringRqueueRedisTemplate).getRedisTemplate();
    doReturn(queueConfigList).when(rqueueSystemManagerService).getSortedQueueConfigs();
    doReturn(Arrays.asList(100L, 110L))
        .when(redisTemplate)
        .executePipelined(any(RedisCallback.class));
    List<List<Object>> response = rqueueQDetailService.getRunningTasks();
    assertEquals(3, response.size());
    List<Object> headers = Arrays.asList("Queue", "Processing [ZSET]", "Size");
    List<Object> row =
        Arrays.asList(queueConfig.getName(), queueConfig.getProcessingQueueName(), 100L);
    List<Object> row2 =
        Arrays.asList(queueConfig2.getName(), queueConfig2.getProcessingQueueName(), 110L);
    assertEquals(Arrays.asList(headers, row, row2), response);
  }

  @Test
  void getDeadLetterTasks() {
    doReturn(redisTemplate).when(stringRqueueRedisTemplate).getRedisTemplate();
    doReturn(queueConfigList).when(rqueueSystemManagerService).getSortedQueueConfigs();
    doReturn(Arrays.asList(100L, 110L))
        .when(redisTemplate)
        .executePipelined(any(RedisCallback.class));
    List<List<Object>> response = rqueueQDetailService.getDeadLetterTasks();
    assertEquals(3, response.size());
    List<Object> headers = Arrays.asList("Queue", "Dead Letter Queue [LIST]", "Size");
    List<Object> row = Arrays.asList(queueConfig.getName(), "test-dlq", 100L);
    List<Object> row2 = Arrays.asList(queueConfig2.getName(), "", "");
    assertEquals(Arrays.asList(headers, row, row2), response);
  }
}
