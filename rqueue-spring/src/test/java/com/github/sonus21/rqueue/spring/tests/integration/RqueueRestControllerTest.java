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

package com.github.sonus21.rqueue.spring.tests.integration;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;

import com.github.sonus21.rqueue.core.EndpointRegistry;
import com.github.sonus21.rqueue.core.RqueueMessage;
import com.github.sonus21.rqueue.core.support.RqueueMessageUtils;
import com.github.sonus21.rqueue.listener.QueueDetail;
import com.github.sonus21.rqueue.models.db.MessageMetadata;
import com.github.sonus21.rqueue.models.enums.ActionType;
import com.github.sonus21.rqueue.models.enums.AggregationType;
import com.github.sonus21.rqueue.models.enums.ChartType;
import com.github.sonus21.rqueue.models.enums.DataType;
import com.github.sonus21.rqueue.models.enums.TableColumnType;
import com.github.sonus21.rqueue.models.request.ChartDataRequest;
import com.github.sonus21.rqueue.models.request.DataDeleteRequest;
import com.github.sonus21.rqueue.models.request.DataTypeRequest;
import com.github.sonus21.rqueue.models.request.DateViewRequest;
import com.github.sonus21.rqueue.models.request.MessageDeleteRequest;
import com.github.sonus21.rqueue.models.request.MessageMoveRequest;
import com.github.sonus21.rqueue.models.request.QueueExploreRequest;
import com.github.sonus21.rqueue.models.response.Action;
import com.github.sonus21.rqueue.models.response.BaseResponse;
import com.github.sonus21.rqueue.models.response.BooleanResponse;
import com.github.sonus21.rqueue.models.response.ChartDataResponse;
import com.github.sonus21.rqueue.models.response.DataSelectorResponse;
import com.github.sonus21.rqueue.models.response.DataViewResponse;
import com.github.sonus21.rqueue.models.response.MessageMoveResponse;
import com.github.sonus21.rqueue.models.response.StringResponse;
import com.github.sonus21.rqueue.models.response.TableColumn;
import com.github.sonus21.rqueue.spring.app.SpringApp;
import com.github.sonus21.rqueue.spring.tests.SpringIntegrationTest;
import com.github.sonus21.rqueue.test.DeleteMessageListener;
import com.github.sonus21.rqueue.test.common.SpringWebTestBase;
import com.github.sonus21.rqueue.test.dto.Email;
import com.github.sonus21.rqueue.test.dto.Job;
import com.github.sonus21.rqueue.test.dto.Notification;
import com.github.sonus21.rqueue.utils.Constants;
import com.github.sonus21.rqueue.utils.TimeoutUtils;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.DisabledIfEnvironmentVariable;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.web.WebAppConfiguration;
import org.springframework.test.web.servlet.MvcResult;

@ContextConfiguration(classes = SpringApp.class)
@Slf4j
@WebAppConfiguration
@TestPropertySource(
    properties = {
        "spring.data.redis.port=7001",
        "mysql.db.name=RqueueRestController",
        "max.workers.count=40",
        "rqueue.web.statistic.history.day=180"
    })
@SpringIntegrationTest
@DisabledIfEnvironmentVariable(named = "RQUEUE_REACTIVE_ENABLED", matches = "true")
class RqueueRestControllerTest extends SpringWebTestBase {

  @Autowired
  private DeleteMessageListener deleteMessageListener;

  @Test
  void verifyChartAndQueueData() throws Exception {
    for (int i = 0; i < 100; i++) {
      Job job = Job.newInstance();
      enqueue(jobQueue, job);
    }
    TimeoutUtils.waitFor(
        () -> getMessageCount(jobQueue) == 0,
        Constants.SECONDS_IN_A_MINUTE * Constants.ONE_MILLI,
        "Job to run");
    verifyChartLatencyData();
    verifyChartStatsData();
    verifyCompletedJobsData();
  }

  void verifyChartLatencyData() throws Exception {
    ChartDataRequest chartDataRequest =
        new ChartDataRequest(ChartType.LATENCY, AggregationType.DAILY);
    MvcResult result =
        this.mockMvc
            .perform(
                post("/rqueue/api/v1/chart")
                    .contentType(MediaType.APPLICATION_JSON)
                    .content(mapper.writeValueAsBytes(chartDataRequest)))
            .andReturn();
    String response = result.getResponse().getContentAsString();
    ChartDataResponse dataResponse = mapper.readValue(response, ChartDataResponse.class);
    assertNull(dataResponse.getMessage());
    assertEquals(0, dataResponse.getCode());
    assertEquals(181, dataResponse.getData().size());
  }

  void verifyChartStatsData() throws Exception {
    ChartDataRequest chartDataRequest =
        new ChartDataRequest(ChartType.STATS, AggregationType.DAILY);
    MvcResult result =
        this.mockMvc
            .perform(
                post("/rqueue/api/v1/chart")
                    .contentType(MediaType.APPLICATION_JSON)
                    .content(mapper.writeValueAsBytes(chartDataRequest)))
            .andReturn();
    String response = result.getResponse().getContentAsString();
    ChartDataResponse dataResponse = mapper.readValue(response, ChartDataResponse.class);
    assertNull(dataResponse.getMessage());
    assertEquals(0, dataResponse.getCode());
    assertEquals(181, dataResponse.getData().size());
  }

  void verifyCompletedJobsData() throws Exception {
    QueueDetail queueDetail = EndpointRegistry.get(jobQueue);
    QueueExploreRequest request = new QueueExploreRequest();
    request.setType(DataType.ZSET);
    request.setSrc(jobQueue);
    request.setName(queueDetail.getCompletedQueueName());

    MvcResult result =
        this.mockMvc
            .perform(
                post("/rqueue/api/v1/queue-data")
                    .contentType(MediaType.APPLICATION_JSON)
                    .content(mapper.writeValueAsBytes(request)))
            .andReturn();

    DataViewResponse dataViewResponse =
        mapper.readValue(result.getResponse().getContentAsString(), DataViewResponse.class);
    assertNull(dataViewResponse.getMessage());
    assertEquals(0, dataViewResponse.getCode());
    assertEquals(20, dataViewResponse.getRows().size());
    assertEquals(4, dataViewResponse.getRows().get(0).getColumns().size());
    assertEquals(
        Collections.singletonList(
            new Action(
                ActionType.DELETE,
                String.format(
                    "Completed messages for queue '%s'", queueDetail.getCompletedQueueName()))),
        dataViewResponse.getActions());
  }

  @Test
  void exploreDataList() throws Exception {
    enqueue(emailDeadLetterQueue, i -> Email.newInstance(), 30, true);
    QueueExploreRequest request = new QueueExploreRequest();
    request.setType(DataType.LIST);
    request.setSrc(emailQueue);
    request.setName(emailDeadLetterQueue);

    MvcResult result =
        this.mockMvc
            .perform(
                post("/rqueue/api/v1/queue-data")
                    .contentType(MediaType.APPLICATION_JSON)
                    .content(mapper.writeValueAsBytes(request)))
            .andReturn();

    DataViewResponse dataViewResponse =
        mapper.readValue(result.getResponse().getContentAsString(), DataViewResponse.class);
    assertNull(dataViewResponse.getMessage());
    assertEquals(0, dataViewResponse.getCode());
    assertEquals(
        Collections.singletonList(
            new Action(
                ActionType.DELETE, String.format("dead letter queue '%s'", emailDeadLetterQueue))),
        dataViewResponse.getActions());
    assertEquals(20, dataViewResponse.getRows().size());
    assertEquals(4, dataViewResponse.getRows().get(0).getColumns().size());
  }

  @Test
  void exploreDataZset() throws Exception {
    for (int i = 0; i < 30; i++) {
      enqueueIn(
          emailQueue, Email.newInstance(), Constants.SECONDS_IN_A_MINUTE * Constants.ONE_MILLI);
    }
    QueueExploreRequest request = new QueueExploreRequest();
    request.setType(DataType.ZSET);
    request.setSrc(emailQueue);
    request.setName(rqueueConfig.getScheduledQueueName(emailQueue));

    MvcResult result =
        this.mockMvc
            .perform(
                post("/rqueue/api/v1/queue-data")
                    .contentType(MediaType.APPLICATION_JSON)
                    .content(mapper.writeValueAsBytes(request)))
            .andReturn();

    DataViewResponse dataViewResponse =
        mapper.readValue(result.getResponse().getContentAsString(), DataViewResponse.class);
    assertNull(dataViewResponse.getMessage());
    assertEquals(0, dataViewResponse.getCode());
    assertEquals(1, dataViewResponse.getActions().size());
    assertEquals(20, dataViewResponse.getRows().size());
    assertEquals(5, dataViewResponse.getRows().get(0).getColumns().size());
  }

  @Test
  void exploreDataProcessingQueue() throws Exception {
    String processingSet = rqueueConfig.getProcessingQueueName(emailQueue);
    for (int i = 0; i < 30; i++) {
      enqueueIn(
          Email.newInstance(), processingSet, Constants.SECONDS_IN_A_MINUTE * Constants.ONE_MILLI);
    }
    QueueExploreRequest request = new QueueExploreRequest();
    request.setType(DataType.ZSET);
    request.setSrc(emailQueue);
    request.setName(processingSet);

    MvcResult result =
        this.mockMvc
            .perform(
                post("/rqueue/api/v1/queue-data")
                    .contentType(MediaType.APPLICATION_JSON)
                    .content(mapper.writeValueAsBytes(request)))
            .andReturn();

    DataViewResponse dataViewResponse =
        mapper.readValue(result.getResponse().getContentAsString(), DataViewResponse.class);
    assertNull(dataViewResponse.getMessage());
    assertEquals(0, dataViewResponse.getCode());
    assertEquals(0, dataViewResponse.getActions().size());
    assertEquals(20, dataViewResponse.getRows().size());
    assertEquals(5, dataViewResponse.getHeaders().size());
    assertEquals(5, dataViewResponse.getRows().get(0).getColumns().size());
  }

  @Test
  void deleteDataSet() throws Exception {
    enqueue(emailDeadLetterQueue, i -> Email.newInstance(), 30, true);
    DataDeleteRequest request = new DataDeleteRequest();
    request.setQueueName(emailQueue);
    request.setDatasetName(emailDeadLetterQueue);
    MvcResult result =
        this.mockMvc
            .perform(
                post("/rqueue/api/v1/delete-queue-part")
                    .contentType(MediaType.APPLICATION_JSON)
                    .content(mapper.writeValueAsBytes(request)))
            .andReturn();
    BooleanResponse booleanResponse =
        mapper.readValue(result.getResponse().getContentAsString(), BooleanResponse.class);
    assertNull(booleanResponse.getMessage());
    assertEquals(0, booleanResponse.getCode());
    assertTrue(booleanResponse.isValue());
    TimeoutUtils.waitFor(
        () -> stringRqueueRedisTemplate.lrange(emailDeadLetterQueue, 0, -1).size() == 0,
        "dead letter queue deletion");
  }

  @Test
  void dataType() throws Exception {
    enqueue(Email.newInstance(), emailDeadLetterQueue);
    DataTypeRequest request = new DataTypeRequest();
    request.setName(emailDeadLetterQueue);
    MvcResult result =
        this.mockMvc
            .perform(
                post("/rqueue/api/v1/data-type")
                    .contentType(MediaType.APPLICATION_JSON)
                    .content(mapper.writeValueAsBytes(request)))
            .andReturn();
    StringResponse response =
        mapper.readValue(result.getResponse().getContentAsString(), StringResponse.class);
    assertNull(response.getMessage());
    assertEquals(0, response.getCode());
    assertEquals("LIST", response.getVal());
  }

  @Test
  void moveMessage() throws Exception {
    enqueue(emailDeadLetterQueue, i -> Email.newInstance(), 30, true);
    MessageMoveRequest request =
        new MessageMoveRequest(emailDeadLetterQueue, DataType.LIST, emailQueue, DataType.LIST);
    MvcResult result =
        this.mockMvc
            .perform(
                post("/rqueue/api/v1/move-data")
                    .contentType(MediaType.APPLICATION_JSON)
                    .content(mapper.writeValueAsBytes(request)))
            .andReturn();
    MessageMoveResponse response =
        mapper.readValue(result.getResponse().getContentAsString(), MessageMoveResponse.class);
    assertNull(response.getMessage());
    assertEquals(0, response.getCode());
    assertEquals(100, response.getNumberOfMessageTransferred());
    assertEquals(Long.valueOf(0), stringRqueueRedisTemplate.getListSize(emailDeadLetterQueue));
  }

  @Test
  void viewData() throws Exception {
    enqueue(emailDeadLetterQueue, i -> Email.newInstance(), 30, true);
    DateViewRequest dateViewRequest = new DateViewRequest();
    dateViewRequest.setName(emailDeadLetterQueue);
    dateViewRequest.setType(DataType.LIST);
    MvcResult result =
        this.mockMvc
            .perform(
                post("/rqueue/api/v1/view-data")
                    .content(mapper.writeValueAsBytes(dateViewRequest))
                    .contentType(MediaType.APPLICATION_JSON))
            .andReturn();
    DataViewResponse dataViewResponse =
        mapper.readValue(result.getResponse().getContentAsString(), DataViewResponse.class);
    assertEquals(0, dataViewResponse.getCode());
    assertEquals(Collections.emptyList(), dataViewResponse.getActions());
    assertEquals(20, dataViewResponse.getRows().size());
  }

  @Test
  void deleteQueue() throws Exception {
    for (int i = 0; i < 30; i++) {
      enqueue(jobQueue, Job.newInstance());
    }
    DataTypeRequest request = new DataTypeRequest();
    request.setName(jobQueue);
    System.out.println(new String(mapper.writeValueAsBytes(request)));
    MvcResult result =
        this.mockMvc
            .perform(
                post("/rqueue/api/v1/delete-queue")
                    .content(mapper.writeValueAsBytes(request))
                    .contentType(MediaType.APPLICATION_JSON))
            .andReturn();
    BaseResponse response =
        mapper.readValue(result.getResponse().getContentAsString(), BaseResponse.class);
    assertEquals(0, response.getCode());
    assertEquals("Queue deleted", response.getMessage());
  }

  @Test
  void deleteMessage() throws Exception {
    Email email = Email.newInstance();
    deleteMessageListener.clear();
    enqueueIn(emailQueue, email, 10 * Constants.ONE_MILLI);
    RqueueMessage message =
        rqueueMessageTemplate
            .readFromZset(rqueueConfig.getScheduledQueueName(emailQueue), 0, -1)
            .get(0);
    MessageDeleteRequest request = new MessageDeleteRequest();
    request.setMessageId(message.getId());
    request.setQueueName(emailQueue);
    MvcResult result =
        this.mockMvc
            .perform(
                post("/rqueue/api/v1/delete-message")
                    .content(mapper.writeValueAsBytes(request))
                    .contentType(MediaType.APPLICATION_JSON))
            .andReturn();
    BooleanResponse response =
        mapper.readValue(result.getResponse().getContentAsString(), BooleanResponse.class);
    assertEquals(0, response.getCode());
    Object metadata =
        stringRqueueRedisTemplate.get(
            RqueueMessageUtils.getMessageMetaId(emailQueue, message.getId()));
    assertTrue(((MessageMetadata) metadata).isDeleted());
    TimeoutUtils.waitFor(
        () -> {
          List<Object> messages = deleteMessageListener.getMessages();
          return messages.size() == 1 && email.equals(messages.get(0));
        },
        30 * Constants.ONE_MILLI,
        "message to be deleted");
  }

  @Test
  void jobsData() throws Exception {
    List<String> messageIds = new ArrayList<>();
    for (int i = 0; i < 30; i++) {
      messageIds.add(rqueueMessageEnqueuer.enqueue(notificationQueue, Notification.newInstance()));
    }
    TimeoutUtils.waitFor(
        () -> getMessageCount(notificationQueue) == 0,
        Constants.SECONDS_IN_A_MINUTE * Constants.ONE_MILLI,
        "notifications to be sent");
    String messageId = messageIds.get(random.nextInt(messageIds.size()));
    MvcResult result =
        this.mockMvc
            .perform(
                get("/rqueue/api/v1/jobs")
                    .param("message-id", messageId)
                    .contentType(MediaType.APPLICATION_JSON))
            .andReturn();
    DataViewResponse response =
        mapper.readValue(result.getResponse().getContentAsString(), DataViewResponse.class);
    assertEquals(0, response.getCode());
    assertEquals(6, response.getHeaders().size());
    assertEquals(1, response.getRows().size());
    assertEquals(6, response.getRows().get(0).getColumns().size());
    for (TableColumn column : response.getRows().get(0).getColumns()) {
      assertNotNull(column.getValue(), column.toString());
      assertEquals(TableColumnType.DISPLAY, column.getType());
    }

    result =
        this.mockMvc
            .perform(
                get("/rqueue/api/v1/jobs")
                    .param("message-id", UUID.randomUUID().toString())
                    .contentType(MediaType.APPLICATION_JSON))
            .andReturn();

    response = mapper.readValue(result.getResponse().getContentAsString(), DataViewResponse.class);
    assertEquals(0, response.getCode());
    assertEquals("No jobs found", response.getMessage());
    assertNull(response.getHeaders());
    assertEquals(0, response.getRows().size());
  }

  @Test
  void aggregatorSelector() throws Exception {
    MvcResult result =
        this.mockMvc
            .perform(
                get("/rqueue/api/v1/aggregate-data-selector")
                    .param("type", AggregationType.DAILY.name())
                    .contentType(MediaType.APPLICATION_JSON))
            .andReturn();

    DataSelectorResponse response =
        mapper.readValue(result.getResponse().getContentAsString(), DataSelectorResponse.class);
    assertEquals(0, response.getCode());
    assertEquals("Select Number of Days", response.getTitle());
    assertEquals(19, response.getData().size());

    result =
        this.mockMvc
            .perform(
                get("/rqueue/api/v1/aggregate-data-selector")
                    .param("type", AggregationType.WEEKLY.name())
                    .contentType(MediaType.APPLICATION_JSON))
            .andReturn();

    response =
        mapper.readValue(result.getResponse().getContentAsString(), DataSelectorResponse.class);
    assertEquals(0, response.getCode());
    assertEquals("Select Number of Weeks", response.getTitle());
    assertEquals(27, response.getData().size(), result.getResponse().getContentAsString());

    result =
        this.mockMvc
            .perform(
                get("/rqueue/api/v1/aggregate-data-selector")
                    .param("type", AggregationType.MONTHLY.name())
                    .contentType(MediaType.APPLICATION_JSON))
            .andReturn();

    response =
        mapper.readValue(result.getResponse().getContentAsString(), DataSelectorResponse.class);
    assertEquals(0, response.getCode());
    assertEquals("Select Number of Months", response.getTitle());
    assertEquals(7, response.getData().size());
  }
}
