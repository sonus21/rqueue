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

package com.github.sonus21.rqueue.web.controller;

import com.github.sonus21.rqueue.config.RqueueConfig;
import com.github.sonus21.rqueue.config.RqueueWebConfig;
import com.github.sonus21.rqueue.models.db.QueueConfig;
import com.github.sonus21.rqueue.models.enums.TaskStatus;
import com.github.sonus21.rqueue.models.enums.AggregationType;
import com.github.sonus21.rqueue.models.enums.DataType;
import com.github.sonus21.rqueue.models.enums.NavTab;
import com.github.sonus21.rqueue.models.response.RedisDataDetail;
import com.github.sonus21.rqueue.utils.StringUtils;
import com.github.sonus21.rqueue.web.service.RqueueQDetailService;
import com.github.sonus21.rqueue.web.service.RqueueSystemManagerService;
import com.github.sonus21.rqueue.web.service.RqueueUtilityService;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Locale;
import java.util.Map.Entry;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import org.jtwig.spring.JtwigViewResolver;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.data.util.Pair;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.servlet.View;

@Controller
@RequestMapping("rqueue")
public class RqueueViewController {
  private final JtwigViewResolver rqueueViewResolver;
  private final RqueueConfig rqueueConfig;
  private final RqueueWebConfig rqueueWebConfig;
  private final RqueueQDetailService rqueueQDetailService;
  private final RqueueUtilityService rqueueUtilityService;
  private final RqueueSystemManagerService rqueueSystemManagerService;

  @Autowired
  public RqueueViewController(
      @Qualifier("rqueueViewResolver") JtwigViewResolver rqueueViewResolver,
      RqueueConfig rqueueConfig,
      RqueueWebConfig rqueueWebConfig,
      RqueueQDetailService rqueueQDetailService,
      RqueueUtilityService rqueueUtilityService,
      RqueueSystemManagerService rqueueSystemManagerService) {
    this.rqueueViewResolver = rqueueViewResolver;
    this.rqueueConfig = rqueueConfig;
    this.rqueueWebConfig = rqueueWebConfig;
    this.rqueueQDetailService = rqueueQDetailService;
    this.rqueueUtilityService = rqueueUtilityService;
    this.rqueueSystemManagerService = rqueueSystemManagerService;
  }

  private void addNavData(Model model, NavTab tab) {
    for (NavTab navTab : NavTab.values()) {
      String name = navTab.name().toLowerCase() + "Active";
      model.addAttribute(name, tab == navTab);
    }
  }

  private void addBasicDetails(Model model, HttpServletRequest request) {
    Pair<String, String> releaseAndVersion = rqueueUtilityService.getLatestVersion();
    model.addAttribute("releaseLink", releaseAndVersion.getFirst());
    model.addAttribute("latestVersion", releaseAndVersion.getSecond());
    model.addAttribute(
        "time", OffsetDateTime.now(ZoneOffset.UTC).format(DateTimeFormatter.ISO_LOCAL_DATE_TIME));
    model.addAttribute("timeInMilli", System.currentTimeMillis());
    model.addAttribute("version", rqueueConfig.getVersion());
    String xForwardedPrefix = request.getHeader("x-forwarded-prefix");
    String prefix = "/";
    if (!StringUtils.isEmpty(xForwardedPrefix)) {
      if (xForwardedPrefix.endsWith("/")) {
        xForwardedPrefix = xForwardedPrefix.substring(0, xForwardedPrefix.length() - 1);
      }
      prefix = xForwardedPrefix + prefix;
    }
    model.addAttribute("urlPrefix", prefix);
  }

  @GetMapping
  public View index(Model model, HttpServletRequest request, HttpServletResponse response)
      throws Exception {
    if (!rqueueWebConfig.isEnable()) {
      response.setStatus(HttpServletResponse.SC_SERVICE_UNAVAILABLE);
      return null;
    }
    addBasicDetails(model, request);
    addNavData(model, null);
    model.addAttribute("title", "Rqueue Dashboard");
    model.addAttribute("aggregatorTypes", Arrays.asList(AggregationType.values()));
    model.addAttribute("typeSelectors", TaskStatus.getActiveChartStatus());
    return rqueueViewResolver.resolveViewName("index", Locale.ENGLISH);
  }

  @GetMapping("queues")
  public View queues(Model model, HttpServletRequest request, HttpServletResponse response)
      throws Exception {
    if (!rqueueWebConfig.isEnable()) {
      response.setStatus(HttpServletResponse.SC_SERVICE_UNAVAILABLE);
      return null;
    }
    addBasicDetails(model, request);
    addNavData(model, NavTab.QUEUES);
    model.addAttribute("title", "Queues");
    List<QueueConfig> queueConfigs = rqueueSystemManagerService.getSortedQueueConfigs();
    List<Entry<String, List<Entry<NavTab, RedisDataDetail>>>> queueNameConfigs =
        new ArrayList<>(rqueueQDetailService.getQueueDataStructureDetails(queueConfigs).entrySet());
    queueNameConfigs.sort(Comparator.comparing(Entry::getKey));
    model.addAttribute("queues", queueConfigs);
    model.addAttribute("queueConfigs", queueNameConfigs);
    return rqueueViewResolver.resolveViewName("queues", Locale.ENGLISH);
  }

  @GetMapping("queues/{queueName}")
  public View queueDetail(
      @PathVariable String queueName,
      Model model,
      HttpServletRequest request,
      HttpServletResponse response)
      throws Exception {
    if (!rqueueWebConfig.isEnable()) {
      response.setStatus(HttpServletResponse.SC_SERVICE_UNAVAILABLE);
      return null;
    }
    QueueConfig queueConfig = rqueueSystemManagerService.getQueueConfig(queueName);
    List<NavTab> queueActions = rqueueQDetailService.getNavTabs(queueConfig);
    List<Entry<NavTab, RedisDataDetail>> queueRedisDataDetail =
        rqueueQDetailService.getQueueDataStructureDetail(queueConfig);
    addBasicDetails(model, request);
    addNavData(model, NavTab.QUEUES);
    model.addAttribute("title", "Queue: " + queueName);
    model.addAttribute("queueName", queueName);
    model.addAttribute("aggregatorTypes", Arrays.asList(AggregationType.values()));
    model.addAttribute("typeSelectors", TaskStatus.getActiveChartStatus());
    model.addAttribute("queueActions", queueActions);
    model.addAttribute("queueRedisDataDetails", queueRedisDataDetail);
    model.addAttribute("config", queueConfig);
    return rqueueViewResolver.resolveViewName("queue_detail", Locale.ENGLISH);
  }

  @GetMapping("running")
  public View running(Model model, HttpServletRequest request, HttpServletResponse response)
      throws Exception {
    if (!rqueueWebConfig.isEnable()) {
      response.setStatus(HttpServletResponse.SC_SERVICE_UNAVAILABLE);
      return null;
    }
    addBasicDetails(model, request);
    addNavData(model, NavTab.RUNNING);
    model.addAttribute("title", "Running Tasks");
    List<List<Object>> l = rqueueQDetailService.getRunningTasks();
    model.addAttribute("tasks", l.subList(1, l.size()));
    model.addAttribute("header", l.get(0));
    return rqueueViewResolver.resolveViewName("running", Locale.ENGLISH);
  }

  @GetMapping("scheduled")
  public View scheduled(Model model, HttpServletRequest request, HttpServletResponse response)
      throws Exception {
    if (!rqueueWebConfig.isEnable()) {
      response.setStatus(HttpServletResponse.SC_SERVICE_UNAVAILABLE);
      return null;
    }
    addBasicDetails(model, request);
    addNavData(model, NavTab.SCHEDULED);
    model.addAttribute("title", "Scheduled Tasks");
    List<List<Object>> l = rqueueQDetailService.getScheduledTasks();
    model.addAttribute("tasks", l.subList(1, l.size()));
    model.addAttribute("header", l.get(0));
    return rqueueViewResolver.resolveViewName("running", Locale.ENGLISH);
  }

  @GetMapping("dead")
  public View dead(Model model, HttpServletRequest request, HttpServletResponse response)
      throws Exception {
    if (!rqueueWebConfig.isEnable()) {
      response.setStatus(HttpServletResponse.SC_SERVICE_UNAVAILABLE);
      return null;
    }
    addBasicDetails(model, request);
    addNavData(model, NavTab.DEAD);
    model.addAttribute("title", "Tasks moved to dead letter queue");
    List<List<Object>> l = rqueueQDetailService.getDeadLetterTasks();

    model.addAttribute("tasks", l.subList(1, l.size()));
    model.addAttribute("header", l.get(0));
    return rqueueViewResolver.resolveViewName("running", Locale.ENGLISH);
  }

  @GetMapping("pending")
  public View pending(Model model, HttpServletRequest request, HttpServletResponse response)
      throws Exception {
    if (!rqueueWebConfig.isEnable()) {
      response.setStatus(HttpServletResponse.SC_SERVICE_UNAVAILABLE);
      return null;
    }
    addBasicDetails(model, request);
    addNavData(model, NavTab.PENDING);
    model.addAttribute("title", "Tasks waiting for execution");
    List<List<Object>> l = rqueueQDetailService.getWaitingTasks();
    model.addAttribute("tasks", l.subList(1, l.size()));
    model.addAttribute("header", l.get(0));
    return rqueueViewResolver.resolveViewName("running", Locale.ENGLISH);
  }

  @GetMapping("utility")
  public View utility(Model model, HttpServletRequest request, HttpServletResponse response)
      throws Exception {
    if (!rqueueWebConfig.isEnable()) {
      response.setStatus(HttpServletResponse.SC_SERVICE_UNAVAILABLE);
      return null;
    }
    addBasicDetails(model, request);
    addNavData(model, NavTab.UTILITY);
    model.addAttribute("title", "Utility");
    model.addAttribute("supportedDataType", DataType.getEnabledDataTypes());
    return rqueueViewResolver.resolveViewName("utility", Locale.ENGLISH);
  }
}
