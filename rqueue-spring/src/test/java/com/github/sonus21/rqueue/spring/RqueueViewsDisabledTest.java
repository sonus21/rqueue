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

package com.github.sonus21.rqueue.spring;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.delete;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.put;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.sonus21.rqueue.common.RqueueRedisTemplate;
import com.github.sonus21.rqueue.exception.TimedOutException;
import com.github.sonus21.rqueue.models.enums.AggregationType;
import com.github.sonus21.rqueue.models.enums.ChartType;
import com.github.sonus21.rqueue.models.enums.DataType;
import com.github.sonus21.rqueue.models.request.ChartDataRequest;
import com.github.sonus21.rqueue.models.request.MessageMoveRequest;
import com.github.sonus21.rqueue.core.RqueueMessageSender;
import com.github.sonus21.rqueue.spring.app.AppWithMetricEnabled;
import com.github.sonus21.rqueue.test.dto.Job;
import com.github.sonus21.test.RqueueSpringTestRunner;
import javax.servlet.http.HttpServletResponse;
import lombok.extern.slf4j.Slf4j;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.MediaType;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.web.WebAppConfiguration;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;
import org.springframework.web.context.WebApplicationContext;

@ContextConfiguration(classes = AppWithMetricEnabled.class)
@RunWith(RqueueSpringTestRunner.class)
@Slf4j
@WebAppConfiguration
@TestPropertySource(
    properties = {
      "spring.redis.port=6388",
      "mysql.db.name=RqueueRestController",
      "rqueue.web.enable=false"
    })
public class RqueueViewsDisabledTest {
  @Autowired private RqueueMessageSender rqueueMessageSender;
  @Autowired private WebApplicationContext wac;

  @Value("${job.queue.name}")
  private String jobQueueName;

  @Value("${notification.queue.name}")
  private String notificationQueue;

  @Value("${email.queue.name}")
  private String emailQueueName;

  @Value("${email.dead.letter.queue.name}")
  private String emailDlq;

  private MockMvc mockMvc;
  private ObjectMapper mapper = new ObjectMapper();

  @Autowired private RqueueRedisTemplate<String> stringRqueueRedisTemplate;

  @Before
  public void init() throws TimedOutException {
    this.mockMvc = MockMvcBuilders.webAppContextSetup(this.wac).build();
  }

  @Test
  public void home() throws Exception {
    assertNull(
        this.mockMvc
            .perform(get("/rqueue"))
            .andExpect(status().is(HttpServletResponse.SC_SERVICE_UNAVAILABLE))
            .andReturn()
            .getModelAndView());
  }

  @Test
  public void queues() throws Exception {
    assertNull(
        this.mockMvc
            .perform(get("/rqueue/queues"))
            .andExpect(status().is(HttpServletResponse.SC_SERVICE_UNAVAILABLE))
            .andReturn()
            .getModelAndView());
  }

  @Test
  public void queueDetail() throws Exception {
    assertNull(
        this.mockMvc
            .perform(get("/rqueue/queues/" + jobQueueName))
            .andExpect(status().is(HttpServletResponse.SC_SERVICE_UNAVAILABLE))
            .andReturn()
            .getModelAndView());
  }

  @Test
  public void running() throws Exception {
    assertNull(
        this.mockMvc
            .perform(get("/rqueue/queues/running"))
            .andExpect(status().is(HttpServletResponse.SC_SERVICE_UNAVAILABLE))
            .andReturn()
            .getModelAndView());
  }

  @Test
  public void scheduled() throws Exception {
    assertNull(
        this.mockMvc
            .perform(get("/rqueue/queues/scheduled"))
            .andExpect(status().is(HttpServletResponse.SC_SERVICE_UNAVAILABLE))
            .andReturn()
            .getModelAndView());
  }

  @Test
  public void dead() throws Exception {
    assertNull(
        this.mockMvc
            .perform(get("/rqueue/queues/dead"))
            .andExpect(status().is(HttpServletResponse.SC_SERVICE_UNAVAILABLE))
            .andReturn()
            .getModelAndView());
  }

  @Test
  public void pending() throws Exception {
    assertNull(
        this.mockMvc
            .perform(get("/rqueue/queues/pending"))
            .andExpect(status().is(HttpServletResponse.SC_SERVICE_UNAVAILABLE))
            .andReturn()
            .getModelAndView());
  }

  @Test
  public void utility() throws Exception {
    assertNull(
        this.mockMvc
            .perform(get("/rqueue/queues/utility"))
            .andExpect(status().is(HttpServletResponse.SC_SERVICE_UNAVAILABLE))
            .andReturn()
            .getModelAndView());
  }

  @Test
  public void testGetChart() throws Exception {
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
  public void testExploreData() throws Exception {
    assertEquals(
        "",
        this.mockMvc
            .perform(
                get("/rqueue/api/v1/explore")
                    .param("type", "LIST")
                    .param("src", emailQueueName)
                    .param("name", emailDlq))
            .andExpect(status().is(HttpServletResponse.SC_SERVICE_UNAVAILABLE))
            .andReturn()
            .getResponse()
            .getContentAsString());
  }

  @Test
  public void deleteDataSet() throws Exception {
    assertEquals(
        "",
        this.mockMvc
            .perform(delete("/rqueue/api/v1/data-set/" + emailDlq))
            .andExpect(status().is(HttpServletResponse.SC_SERVICE_UNAVAILABLE))
            .andReturn()
            .getResponse()
            .getContentAsString());
  }

  @Test
  public void dataType() throws Exception {
    assertEquals(
        "",
        this.mockMvc
            .perform(get("/rqueue/api/v1/data-type").param("name", emailDlq))
            .andExpect(status().is(HttpServletResponse.SC_SERVICE_UNAVAILABLE))
            .andReturn()
            .getResponse()
            .getContentAsString());
  }

  @Test
  public void moveMessage() throws Exception {
    MessageMoveRequest request =
        new MessageMoveRequest(emailDlq, DataType.LIST, emailQueueName, DataType.LIST);

    assertEquals(
        "",
        this.mockMvc
            .perform(
                put("/rqueue/api/v1/move")
                    .contentType(MediaType.APPLICATION_JSON)
                    .content(mapper.writeValueAsString(request)))
            .andExpect(status().is(HttpServletResponse.SC_SERVICE_UNAVAILABLE))
            .andReturn()
            .getResponse()
            .getContentAsString());
  }

  @Test
  public void viewData() throws Exception {
    assertEquals(
        "",
        this.mockMvc
            .perform(
                get("/rqueue/api/v1/data")
                    .param("name", emailDlq)
                    .param("type", DataType.LIST.name())
                    .contentType(MediaType.APPLICATION_JSON))
            .andExpect(status().is(HttpServletResponse.SC_SERVICE_UNAVAILABLE))
            .andReturn()
            .getResponse()
            .getContentAsString());
  }

  @Test
  public void deleteQueue() throws Exception {
    assertEquals(
        "",
        this.mockMvc
            .perform(
                delete("/rqueue/api/v1/queues/" + jobQueueName)
                    .contentType(MediaType.APPLICATION_JSON))
            .andExpect(status().is(HttpServletResponse.SC_SERVICE_UNAVAILABLE))
            .andReturn()
            .getResponse()
            .getContentAsString());
  }

  @Test
  public void deleteMessage() throws Exception {
    Job job = Job.newInstance();
    assertEquals(
        "",
        this.mockMvc
            .perform(
                delete("/rqueue/api/v1/data-set/" + jobQueueName + "/" + job.getId())
                    .contentType(MediaType.APPLICATION_JSON))
            .andExpect(status().is(HttpServletResponse.SC_SERVICE_UNAVAILABLE))
            .andReturn()
            .getResponse()
            .getContentAsString());
  }
}
