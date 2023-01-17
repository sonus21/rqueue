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
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import com.github.sonus21.rqueue.models.enums.AggregationType;
import com.github.sonus21.rqueue.models.enums.ChartType;
import com.github.sonus21.rqueue.models.enums.DataType;
import com.github.sonus21.rqueue.models.request.ChartDataRequest;
import com.github.sonus21.rqueue.models.request.DataDeleteRequest;
import com.github.sonus21.rqueue.models.request.DataTypeRequest;
import com.github.sonus21.rqueue.models.request.DateViewRequest;
import com.github.sonus21.rqueue.models.request.MessageDeleteRequest;
import com.github.sonus21.rqueue.models.request.MessageMoveRequest;
import com.github.sonus21.rqueue.models.request.QueueExploreRequest;
import com.github.sonus21.rqueue.spring.app.SpringApp;
import com.github.sonus21.rqueue.spring.tests.SpringIntegrationTest;
import com.github.sonus21.rqueue.test.common.SpringWebTestBase;
import com.github.sonus21.rqueue.test.dto.Job;
import jakarta.servlet.http.HttpServletResponse;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.DisabledIfEnvironmentVariable;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.springframework.http.MediaType;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.web.WebAppConfiguration;

@ContextConfiguration(classes = SpringApp.class)
@Slf4j
@WebAppConfiguration
@TestPropertySource(
    properties = {
        "spring.data.redis.port=7003",
        "mysql.db.name=RqueueRestController",
        "rqueue.web.enable=false"
    })
@SpringIntegrationTest
@DisabledIfEnvironmentVariable(named = "RQUEUE_REACTIVE_ENABLED", matches = "true")
class RqueueWebDisabledTest extends SpringWebTestBase {

  @ParameterizedTest
  @ValueSource(
      strings = {
          "",
          "/queues",
          "/running",
          "/scheduled",
          "/dead",
          "/pending",
          "/utility",
          "/queues/test-queue",
          "/api/v1/aggregate-data-selector?type=WEEKLY",
          "/api/v1/jobs?message-id=1234567890"
      })
  void pathTester(String path) throws Exception {
    assertNull(
        this.mockMvc
            .perform(get("/rqueue" + path))
            .andExpect(status().is(HttpServletResponse.SC_SERVICE_UNAVAILABLE))
            .andReturn()
            .getModelAndView());
  }

  @Test
  void getChart() throws Exception {
    ChartDataRequest chartDataRequest =
        new ChartDataRequest(ChartType.STATS, AggregationType.DAILY);
    assertEquals(
        "",
        this.mockMvc
            .perform(
                post("/rqueue/api/v1/chart")
                    .contentType(MediaType.APPLICATION_JSON)
                    .content(mapper.writeValueAsBytes(chartDataRequest)))
            .andExpect(status().is(HttpServletResponse.SC_SERVICE_UNAVAILABLE))
            .andReturn()
            .getResponse()
            .getContentAsString());
  }

  @Test
  void exploreData() throws Exception {
    QueueExploreRequest request = new QueueExploreRequest();
    request.setType(DataType.LIST);
    request.setSrc(emailQueue);
    request.setName(emailDeadLetterQueue);
    assertEquals(
        "",
        this.mockMvc
            .perform(
                post("/rqueue/api/v1/queue-data")
                    .contentType(MediaType.APPLICATION_JSON)
                    .content(mapper.writeValueAsBytes(request)))
            .andExpect(status().is(HttpServletResponse.SC_SERVICE_UNAVAILABLE))
            .andReturn()
            .getResponse()
            .getContentAsString());
  }

  @Test
  void deleteDataSet() throws Exception {
    DataDeleteRequest request = new DataDeleteRequest();
    request.setQueueName(emailQueue);
    request.setDatasetName(emailDeadLetterQueue);
    assertEquals(
        "",
        this.mockMvc
            .perform(
                post("/rqueue/api/v1/delete-queue-part")
                    .contentType(MediaType.APPLICATION_JSON)
                    .content(mapper.writeValueAsBytes(request)))
            .andExpect(status().is(HttpServletResponse.SC_SERVICE_UNAVAILABLE))
            .andReturn()
            .getResponse()
            .getContentAsString());
  }

  @Test
  void dataType() throws Exception {
    DataTypeRequest request = new DataTypeRequest();
    request.setName(emailDeadLetterQueue);
    assertEquals(
        "",
        this.mockMvc
            .perform(
                post("/rqueue/api/v1/data-type")
                    .contentType(MediaType.APPLICATION_JSON)
                    .content(mapper.writeValueAsBytes(request)))
            .andExpect(status().is(HttpServletResponse.SC_SERVICE_UNAVAILABLE))
            .andReturn()
            .getResponse()
            .getContentAsString());
  }

  @Test
  void moveMessage() throws Exception {
    MessageMoveRequest request =
        new MessageMoveRequest(emailDeadLetterQueue, DataType.LIST, emailQueue, DataType.LIST);
    assertEquals(
        "",
        this.mockMvc
            .perform(
                post("/rqueue/api/v1/move-data")
                    .contentType(MediaType.APPLICATION_JSON)
                    .content(mapper.writeValueAsString(request)))
            .andExpect(status().is(HttpServletResponse.SC_SERVICE_UNAVAILABLE))
            .andReturn()
            .getResponse()
            .getContentAsString());
  }

  @Test
  void viewData() throws Exception {
    DateViewRequest dateViewRequest = new DateViewRequest();
    dateViewRequest.setName(emailDeadLetterQueue);
    dateViewRequest.setType(DataType.LIST);
    assertEquals(
        "",
        this.mockMvc
            .perform(
                post("/rqueue/api/v1/view-data")
                    .content(mapper.writeValueAsBytes(dateViewRequest))
                    .contentType(MediaType.APPLICATION_JSON))
            .andExpect(status().is(HttpServletResponse.SC_SERVICE_UNAVAILABLE))
            .andReturn()
            .getResponse()
            .getContentAsString());
  }

  @Test
  void deleteQueue() throws Exception {
    DataTypeRequest request = new DataTypeRequest();
    request.setName(jobQueue);
    assertEquals(
        "",
        this.mockMvc
            .perform(
                post("/rqueue/api/v1/delete-queue")
                    .content(mapper.writeValueAsBytes(request))
                    .contentType(MediaType.APPLICATION_JSON))
            .andExpect(status().is(HttpServletResponse.SC_SERVICE_UNAVAILABLE))
            .andReturn()
            .getResponse()
            .getContentAsString());
  }

  @Test
  void deleteMessage() throws Exception {
    Job job = Job.newInstance();
    MessageDeleteRequest request = new MessageDeleteRequest();
    request.setMessageId(job.getId());
    request.setQueueName(emailQueue);
    assertEquals(
        "",
        this.mockMvc
            .perform(
                post("/rqueue/api/v1/delete-message")
                    .content(mapper.writeValueAsBytes(request))
                    .contentType(MediaType.APPLICATION_JSON))
            .andExpect(status().is(HttpServletResponse.SC_SERVICE_UNAVAILABLE))
            .andReturn()
            .getResponse()
            .getContentAsString());
  }
}
