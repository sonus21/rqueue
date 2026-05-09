/*
 * Copyright (c) 2020-2026 Sonu Kumar
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
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;

import com.github.sonus21.rqueue.models.enums.NavTab;
import com.github.sonus21.rqueue.spring.app.SpringApp;
import com.github.sonus21.rqueue.spring.tests.SpringIntegrationTest;
import com.github.sonus21.rqueue.test.common.SpringWebTestBase;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.DisabledIfEnvironmentVariable;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.web.WebAppConfiguration;
import org.springframework.test.web.servlet.MvcResult;

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

  private String body(MvcResult result) throws Exception {
    return result.getResponse().getContentAsString();
  }

  private void verifyHtmlResponse(MvcResult result) throws Exception {
    assertEquals(200, result.getResponse().getStatus());
    assertTrue(
        result.getResponse().getContentType().startsWith("text/html"),
        "Expected text/html content type");
  }

  private void verifyTitle(MvcResult result, String expectedTitle) throws Exception {
    assertTrue(
        body(result).contains("<title>" + expectedTitle + "</title>"),
        "Expected title: " + expectedTitle);
  }

  private void verifyNavActive(MvcResult result, NavTab navTab) throws Exception {
    if (navTab == null) {
      return;
    }
    String path = navTab.name().toLowerCase();
    String body = body(result);
    assertTrue(
        body.contains("class=\"active\"") && body.contains("rqueue/" + path),
        "Expected nav tab " + path + " to be active");
  }

  @Test
  void home() throws Exception {
    MvcResult result = this.mockMvc.perform(get("/rqueue")).andReturn();
    verifyHtmlResponse(result);
    verifyTitle(result, "Rqueue Dashboard");
  }

  @Test
  void queues() throws Exception {
    MvcResult result = this.mockMvc.perform(get("/rqueue/queues")).andReturn();
    verifyHtmlResponse(result);
    verifyTitle(result, "Queues");
    verifyNavActive(result, NavTab.QUEUES);
  }

  @Test
  void queueDetail() throws Exception {
    MvcResult result = this.mockMvc.perform(get("/rqueue/queues/" + jobQueue)).andReturn();
    verifyHtmlResponse(result);
    verifyTitle(result, "Queue: " + jobQueue);
    verifyNavActive(result, NavTab.QUEUES);
  }

  @Test
  void running() throws Exception {
    MvcResult result = this.mockMvc.perform(get("/rqueue/running")).andReturn();
    verifyHtmlResponse(result);
    verifyTitle(result, "Running Tasks");
    verifyNavActive(result, NavTab.RUNNING);
  }

  @Test
  void scheduled() throws Exception {
    MvcResult result = this.mockMvc.perform(get("/rqueue/scheduled")).andReturn();
    verifyHtmlResponse(result);
    verifyTitle(result, "Scheduled Tasks");
    verifyNavActive(result, NavTab.SCHEDULED);
  }

  @Test
  void dead() throws Exception {
    MvcResult result = this.mockMvc.perform(get("/rqueue/dead")).andReturn();
    verifyHtmlResponse(result);
    verifyTitle(result, "Tasks moved to dead letter queue");
    verifyNavActive(result, NavTab.DEAD);
  }

  @Test
  void pending() throws Exception {
    MvcResult result = this.mockMvc.perform(get("/rqueue/pending")).andReturn();
    verifyHtmlResponse(result);
    verifyTitle(result, "Tasks waiting for execution");
    verifyNavActive(result, NavTab.PENDING);
  }

  @Test
  void utility() throws Exception {
    MvcResult result = this.mockMvc.perform(get("/rqueue/utility")).andReturn();
    verifyHtmlResponse(result);
    verifyTitle(result, "Utility");
    verifyNavActive(result, NavTab.UTILITY);
  }
}
