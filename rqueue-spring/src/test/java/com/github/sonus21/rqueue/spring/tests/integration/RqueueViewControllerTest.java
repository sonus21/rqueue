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

package com.github.sonus21.rqueue.spring.tests.integration;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;

import com.github.sonus21.rqueue.exception.TimedOutException;
import com.github.sonus21.rqueue.models.db.TaskStatus;
import com.github.sonus21.rqueue.models.enums.AggregationType;
import com.github.sonus21.rqueue.models.enums.NavTab;
import com.github.sonus21.rqueue.spring.app.SpringApp;
import com.github.sonus21.rqueue.test.tests.SpringTestBase;
import com.github.sonus21.test.RqueueSpringTestRunner;
import java.util.Arrays;
import lombok.extern.slf4j.Slf4j;
import org.jtwig.spring.JtwigView;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.web.WebAppConfiguration;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.MvcResult;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;
import org.springframework.ui.ModelMap;
import org.springframework.web.context.WebApplicationContext;

@ContextConfiguration(classes = SpringApp.class)
@RunWith(RqueueSpringTestRunner.class)
@Slf4j
@WebAppConfiguration
@TestPropertySource(
    properties = {
      "spring.redis.port=7002",
      "mysql.db.name=RqueueViewControllerTest",
      "max.workers.count=40",
      "notification.queue.active=false",
      "rqueue.web.statistic.history.day=180",
    })
public class RqueueViewControllerTest extends SpringTestBase {
  @Autowired private WebApplicationContext wac;

  private MockMvc mockMvc;

  @Before
  public void init() throws TimedOutException {
    this.mockMvc = MockMvcBuilders.webAppContextSetup(this.wac).build();
  }

  public void verifyBasicData(ModelMap model, NavTab navTab) {
    assertNotNull(model.get("latestVersion"));
    assertNotNull(model.get("version"));
    assertNotNull(model.get("mavenRepoLink"));
    assertNotNull(model.get("time"));
    assertNotNull(model.get("timeInMilli"));
    for (NavTab tab : NavTab.values()) {
      assertEquals(
          tab.name().toLowerCase() + "Active",
          tab == navTab,
          model.get(tab.name().toLowerCase() + "Active"));
    }
  }

  @Test
  public void home() throws Exception {
    MvcResult result = this.mockMvc.perform(get("/rqueue")).andReturn();
    ModelMap model = result.getModelAndView().getModelMap();
    JtwigView jtwigView = (JtwigView) result.getModelAndView().getView();
    assertEquals("classpath:/templates/rqueue/index.html", jtwigView.getUrl());
    verifyBasicData(model, null);
    assertEquals(
        Arrays.asList(AggregationType.DAILY, AggregationType.WEEKLY, AggregationType.MONTHLY),
        model.get("aggregatorTypes"));
    assertEquals(
        Arrays.asList(
            TaskStatus.MOVED_TO_DLQ,
            TaskStatus.SUCCESSFUL,
            TaskStatus.DISCARDED,
            TaskStatus.RETRIED),
        model.get("typeSelectors"));
  }

  @Test
  public void queues() throws Exception {
    MvcResult result = this.mockMvc.perform(get("/rqueue/queues")).andReturn();
    ModelMap model = result.getModelAndView().getModelMap();
    JtwigView jtwigView = (JtwigView) result.getModelAndView().getView();
    assertEquals("classpath:/templates/rqueue/queues.html", jtwigView.getUrl());
    verifyBasicData(model, NavTab.QUEUES);
    assertEquals("Queues", model.get("title"));
    assertNotNull(model.get("queues"));
    assertNotNull(model.get("queueConfigs"));
  }

  @Test
  public void queueDetail() throws Exception {
    MvcResult result = this.mockMvc.perform(get("/rqueue/queues/" + jobQueue)).andReturn();
    ModelMap model = result.getModelAndView().getModelMap();
    JtwigView jtwigView = (JtwigView) result.getModelAndView().getView();
    assertEquals("classpath:/templates/rqueue/queue_detail.html", jtwigView.getUrl());
    verifyBasicData(model, NavTab.QUEUES);
    assertEquals("Queue: " + jobQueue, model.get("title"));
    assertEquals(jobQueue, model.get("queueName"));
    assertEquals(
        Arrays.asList(AggregationType.DAILY, AggregationType.WEEKLY, AggregationType.MONTHLY),
        model.get("aggregatorTypes"));
    assertEquals(
        Arrays.asList(
            TaskStatus.MOVED_TO_DLQ,
            TaskStatus.SUCCESSFUL,
            TaskStatus.DISCARDED,
            TaskStatus.RETRIED),
        model.get("typeSelectors"));
    assertNotNull(model.get("config"));
    assertNotNull(model.get("queueRedisDataDetails"));
    assertEquals(
        Arrays.asList(NavTab.PENDING, NavTab.SCHEDULED, NavTab.RUNNING), model.get("queueActions"));
  }

  @Test
  public void running() throws Exception {
    MvcResult result = this.mockMvc.perform(get("/rqueue/running")).andReturn();
    ModelMap model = result.getModelAndView().getModelMap();
    JtwigView jtwigView = (JtwigView) result.getModelAndView().getView();
    assertEquals("classpath:/templates/rqueue/running.html", jtwigView.getUrl());
    verifyBasicData(model, NavTab.RUNNING);
    assertEquals("Running Tasks", model.get("title"));
    assertNotNull(model.get("tasks"));
    assertNotNull(model.get("header"));
  }

  @Test
  public void scheduled() throws Exception {
    MvcResult result = this.mockMvc.perform(get("/rqueue/scheduled")).andReturn();
    ModelMap model = result.getModelAndView().getModelMap();
    JtwigView jtwigView = (JtwigView) result.getModelAndView().getView();
    assertEquals("classpath:/templates/rqueue/running.html", jtwigView.getUrl());
    verifyBasicData(model, NavTab.SCHEDULED);
    assertEquals("Scheduled Tasks", model.get("title"));
    assertNotNull(model.get("tasks"));
    assertNotNull(model.get("header"));
  }

  @Test
  public void dead() throws Exception {
    MvcResult result = this.mockMvc.perform(get("/rqueue/dead")).andReturn();
    ModelMap model = result.getModelAndView().getModelMap();
    JtwigView jtwigView = (JtwigView) result.getModelAndView().getView();
    assertEquals("classpath:/templates/rqueue/running.html", jtwigView.getUrl());
    verifyBasicData(model, NavTab.DEAD);
    assertEquals("Tasks moved to dead letter queue", model.get("title"));
    assertNotNull(model.get("tasks"));
    assertNotNull(model.get("header"));
  }

  @Test
  public void pending() throws Exception {
    MvcResult result = this.mockMvc.perform(get("/rqueue/pending")).andReturn();
    ModelMap model = result.getModelAndView().getModelMap();
    JtwigView jtwigView = (JtwigView) result.getModelAndView().getView();
    assertEquals("classpath:/templates/rqueue/running.html", jtwigView.getUrl());
    verifyBasicData(model, NavTab.PENDING);
    assertEquals("Tasks waiting for execution", model.get("title"));
    assertNotNull(model.get("tasks"));
    assertNotNull(model.get("header"));
  }

  @Test
  public void utility() throws Exception {
    MvcResult result = this.mockMvc.perform(get("/rqueue/utility")).andReturn();
    ModelMap model = result.getModelAndView().getModelMap();
    JtwigView jtwigView = (JtwigView) result.getModelAndView().getView();
    assertEquals("classpath:/templates/rqueue/utility.html", jtwigView.getUrl());
    verifyBasicData(model, NavTab.UTILITY);
    assertEquals("Utility", model.get("title"));
    assertNotNull(model.get("supportedDataType"));
  }
}
