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
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;

import com.github.sonus21.rqueue.models.enums.AggregationType;
import com.github.sonus21.rqueue.models.enums.ChartDataType;
import com.github.sonus21.rqueue.models.enums.NavTab;
import com.github.sonus21.rqueue.spring.app.SpringApp;
import com.github.sonus21.rqueue.spring.tests.SpringIntegrationTest;
import com.github.sonus21.rqueue.test.common.SpringWebTestBase;
import io.pebbletemplates.spring.servlet.PebbleView;
import java.util.Arrays;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.DisabledIfEnvironmentVariable;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.web.WebAppConfiguration;
import org.springframework.test.web.servlet.MvcResult;
import org.springframework.ui.ModelMap;

@ContextConfiguration(classes = SpringApp.class)
@Slf4j
@WebAppConfiguration
@TestPropertySource(
    properties = {
        "spring.data.redis.port=7002",
        "mysql.db.name=RqueueViewControllerTest",
        "max.workers.count=40",
        "notification.queue.active=false",
        "rqueue.web.statistic.history.day=180",
    })
@SpringIntegrationTest
@DisabledIfEnvironmentVariable(named = "RQUEUE_REACTIVE_ENABLED", matches = "true")
class RqueueViewControllerTest extends SpringWebTestBase {

  private void verifyBasicData(ModelMap model, NavTab navTab) {
    assertNotNull(model.get("latestVersion"));
    assertNotNull(model.get("version"));
    assertNotNull(model.get("releaseLink"));
    assertNotNull(model.get("time"));
    assertNotNull(model.get("timeInMilli"));
    for (NavTab tab : NavTab.values()) {
      assertEquals(tab == navTab, model.get(tab.name().toLowerCase() + "Active"));
    }
  }

  @Test
  void home() throws Exception {
    MvcResult result = this.mockMvc.perform(get("/rqueue")).andReturn();
    ModelMap model = result.getModelAndView().getModelMap();
    PebbleView pebbleView = (PebbleView) result.getModelAndView().getView();
    assertEquals("templates/rqueue/index.html", pebbleView.getUrl());
    verifyBasicData(model, null);
    assertEquals(
        Arrays.asList(AggregationType.DAILY, AggregationType.WEEKLY, AggregationType.MONTHLY),
        model.get("aggregatorTypes"));
    assertEquals(
        Arrays.asList(
            ChartDataType.SUCCESSFUL,
            ChartDataType.DISCARDED,
            ChartDataType.MOVED_TO_DLQ,
            ChartDataType.RETRIED),
        model.get("typeSelectors"));
  }

  @Test
  void queues() throws Exception {
    MvcResult result = this.mockMvc.perform(get("/rqueue/queues")).andReturn();
    ModelMap model = result.getModelAndView().getModelMap();
    PebbleView pebbleView = (PebbleView) result.getModelAndView().getView();
    assertEquals("templates/rqueue/queues.html", pebbleView.getUrl());
    verifyBasicData(model, NavTab.QUEUES);
    assertEquals("Queues", model.get("title"));
    assertNotNull(model.get("queues"));
    assertNotNull(model.get("queueConfigs"));
  }

  @Test
  void queueDetail() throws Exception {
    MvcResult result = this.mockMvc.perform(get("/rqueue/queues/" + jobQueue)).andReturn();
    ModelMap model = result.getModelAndView().getModelMap();
    PebbleView pebbleView = (PebbleView) result.getModelAndView().getView();
    assertEquals("templates/rqueue/queue_detail.html", pebbleView.getUrl());
    verifyBasicData(model, NavTab.QUEUES);
    assertEquals("Queue: " + jobQueue, model.get("title"));
    assertEquals(jobQueue, model.get("queueName"));
    assertEquals(
        Arrays.asList(AggregationType.DAILY, AggregationType.WEEKLY, AggregationType.MONTHLY),
        model.get("aggregatorTypes"));
    assertEquals(
        Arrays.asList(
            ChartDataType.SUCCESSFUL,
            ChartDataType.DISCARDED,
            ChartDataType.MOVED_TO_DLQ,
            ChartDataType.RETRIED),
        model.get("typeSelectors"));
    assertNotNull(model.get("config"));
    assertNotNull(model.get("queueRedisDataDetails"));
    assertEquals(
        Arrays.asList(NavTab.PENDING, NavTab.SCHEDULED, NavTab.RUNNING), model.get("queueActions"));
  }

  @Test
  void running() throws Exception {
    MvcResult result = this.mockMvc.perform(get("/rqueue/running")).andReturn();
    ModelMap model = result.getModelAndView().getModelMap();
    PebbleView pebbleView = (PebbleView) result.getModelAndView().getView();
    assertEquals("templates/rqueue/running.html", pebbleView.getUrl());
    verifyBasicData(model, NavTab.RUNNING);
    assertEquals("Running Tasks", model.get("title"));
    assertNotNull(model.get("tasks"));
    assertNotNull(model.get("header"));
  }

  @Test
  void scheduled() throws Exception {
    MvcResult result = this.mockMvc.perform(get("/rqueue/scheduled")).andReturn();
    ModelMap model = result.getModelAndView().getModelMap();
    PebbleView pebbleView = (PebbleView) result.getModelAndView().getView();
    assertEquals("templates/rqueue/running.html", pebbleView.getUrl());
    verifyBasicData(model, NavTab.SCHEDULED);
    assertEquals("Scheduled Tasks", model.get("title"));
    assertNotNull(model.get("tasks"));
    assertNotNull(model.get("header"));
  }

  @Test
  void dead() throws Exception {
    MvcResult result = this.mockMvc.perform(get("/rqueue/dead")).andReturn();
    ModelMap model = result.getModelAndView().getModelMap();
    PebbleView pebbleView = (PebbleView) result.getModelAndView().getView();
    assertEquals("templates/rqueue/running.html", pebbleView.getUrl());
    verifyBasicData(model, NavTab.DEAD);
    assertEquals("Tasks moved to dead letter queue", model.get("title"));
    assertNotNull(model.get("tasks"));
    assertNotNull(model.get("header"));
  }

  @Test
  void pending() throws Exception {
    MvcResult result = this.mockMvc.perform(get("/rqueue/pending")).andReturn();
    ModelMap model = result.getModelAndView().getModelMap();
    PebbleView pebbleView = (PebbleView) result.getModelAndView().getView();
    assertEquals("templates/rqueue/running.html", pebbleView.getUrl());
    verifyBasicData(model, NavTab.PENDING);
    assertEquals("Tasks waiting for execution", model.get("title"));
    assertNotNull(model.get("tasks"));
    assertNotNull(model.get("header"));
  }

  @Test
  void utility() throws Exception {
    MvcResult result = this.mockMvc.perform(get("/rqueue/utility")).andReturn();
    ModelMap model = result.getModelAndView().getModelMap();
    PebbleView pebbleView = (PebbleView) result.getModelAndView().getView();
    assertEquals("templates/rqueue/utility.html", pebbleView.getUrl());
    verifyBasicData(model, NavTab.UTILITY);
    assertEquals("Utility", model.get("title"));
    assertNotNull(model.get("supportedDataType"));
  }
}
