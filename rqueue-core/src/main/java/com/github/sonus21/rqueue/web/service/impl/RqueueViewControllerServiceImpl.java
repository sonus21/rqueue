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

package com.github.sonus21.rqueue.web.service.impl;

import com.github.sonus21.rqueue.config.RqueueConfig;
import com.github.sonus21.rqueue.config.RqueueWebConfig;
import com.github.sonus21.rqueue.models.Pair;
import com.github.sonus21.rqueue.models.db.QueueConfig;
import com.github.sonus21.rqueue.models.enums.AggregationType;
import com.github.sonus21.rqueue.models.enums.ChartDataType;
import com.github.sonus21.rqueue.models.enums.DataType;
import com.github.sonus21.rqueue.models.enums.NavTab;
import com.github.sonus21.rqueue.models.response.RedisDataDetail;
import com.github.sonus21.rqueue.utils.DateTimeUtils;
import com.github.sonus21.rqueue.web.service.RqueueQDetailService;
import com.github.sonus21.rqueue.web.service.RqueueSystemManagerService;
import com.github.sonus21.rqueue.web.service.RqueueUtilityService;
import com.github.sonus21.rqueue.web.service.RqueueViewControllerService;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map.Entry;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.ui.Model;

@Service
public class RqueueViewControllerServiceImpl implements RqueueViewControllerService {

  private final RqueueConfig rqueueConfig;
  private final RqueueWebConfig rqueueWebConfig;
  private final RqueueQDetailService rqueueQDetailService;
  private final RqueueUtilityService rqueueUtilityService;
  private final RqueueSystemManagerService rqueueSystemManagerService;

  @Autowired
  public RqueueViewControllerServiceImpl(
      RqueueConfig rqueueConfig,
      RqueueWebConfig rqueueWebConfig,
      RqueueQDetailService rqueueQDetailService,
      RqueueUtilityService rqueueUtilityService,
      RqueueSystemManagerService rqueueSystemManagerService) {
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

  private void addBasicDetails(Model model, String xForwardedPrefix) {
    Pair<String, String> releaseAndVersion = rqueueUtilityService.getLatestVersion();
    model.addAttribute("releaseLink", releaseAndVersion.getFirst());
    model.addAttribute("latestVersion", releaseAndVersion.getSecond());
    model.addAttribute("time", DateTimeUtils.currentTimeFormatted());
    model.addAttribute("timeInMilli", System.currentTimeMillis());
    model.addAttribute("version", rqueueConfig.getLibVersion());
    model.addAttribute("urlPrefix", rqueueWebConfig.getUrlPrefix(xForwardedPrefix));
  }

  @Override
  public void index(Model model, String xForwardedPrefix) {
    addBasicDetails(model, xForwardedPrefix);
    addNavData(model, null);
    model.addAttribute("title", "Rqueue Dashboard");
    model.addAttribute("aggregatorTypes", Arrays.asList(AggregationType.values()));
    model.addAttribute(
        "aggregatorDateCounter", rqueueUtilityService.aggregateDataCounter(AggregationType.DAILY));
    model.addAttribute("typeSelectors", ChartDataType.getActiveCharts());
  }

  @Override
  public void queues(Model model, String xForwardedPrefix) {
    addBasicDetails(model, xForwardedPrefix);
    addNavData(model, NavTab.QUEUES);
    model.addAttribute("title", "Queues");
    List<QueueConfig> queueConfigs = rqueueSystemManagerService.getSortedQueueConfigs();
    List<Entry<String, List<Entry<NavTab, RedisDataDetail>>>> queueNameConfigs =
        new ArrayList<>(rqueueQDetailService.getQueueDataStructureDetails(queueConfigs).entrySet());
    queueNameConfigs.sort(Entry.comparingByKey());
    model.addAttribute("queues", queueConfigs);
    model.addAttribute("queueConfigs", queueNameConfigs);
  }

  @Override
  public void queueDetail(Model model, String xForwardedPrefix, String queueName) {
    QueueConfig queueConfig = rqueueSystemManagerService.getQueueConfig(queueName);
    List<NavTab> queueActions = rqueueQDetailService.getNavTabs(queueConfig);
    List<Entry<NavTab, RedisDataDetail>> queueRedisDataDetail =
        rqueueQDetailService.getQueueDataStructureDetail(queueConfig);
    addBasicDetails(model, xForwardedPrefix);
    addNavData(model, NavTab.QUEUES);
    model.addAttribute("title", "Queue: " + queueName);
    model.addAttribute("queueName", queueName);
    model.addAttribute("aggregatorTypes", Arrays.asList(AggregationType.values()));
    model.addAttribute(
        "aggregatorDateCounter", rqueueUtilityService.aggregateDataCounter(AggregationType.DAILY));
    model.addAttribute("typeSelectors", ChartDataType.getActiveCharts());
    model.addAttribute("queueActions", queueActions);
    model.addAttribute("queueRedisDataDetails", queueRedisDataDetail);
    model.addAttribute("config", queueConfig);
  }

  @Override
  public void running(Model model, String xForwardedPrefix) {
    addBasicDetails(model, xForwardedPrefix);
    addNavData(model, NavTab.RUNNING);
    model.addAttribute("title", "Running Tasks");
    List<List<Object>> l = rqueueQDetailService.getRunningTasks();
    model.addAttribute("tasks", l.subList(1, l.size()));
    model.addAttribute("header", l.get(0));
  }

  @Override
  public void scheduled(Model model, String xForwardedPrefix) {
    addBasicDetails(model, xForwardedPrefix);
    addNavData(model, NavTab.SCHEDULED);
    model.addAttribute("title", "Scheduled Tasks");
    List<List<Object>> l = rqueueQDetailService.getScheduledTasks();
    model.addAttribute("tasks", l.subList(1, l.size()));
    model.addAttribute("header", l.get(0));
  }

  @Override
  public void dead(Model model, String xForwardedPrefix) {
    addBasicDetails(model, xForwardedPrefix);
    addNavData(model, NavTab.DEAD);
    model.addAttribute("title", "Tasks moved to dead letter queue");
    List<List<Object>> l = rqueueQDetailService.getDeadLetterTasks();

    model.addAttribute("tasks", l.subList(1, l.size()));
    model.addAttribute("header", l.get(0));
  }

  @Override
  public void pending(Model model, String xForwardedPrefix) {
    addBasicDetails(model, xForwardedPrefix);
    addNavData(model, NavTab.PENDING);
    model.addAttribute("title", "Tasks waiting for execution");
    List<List<Object>> l = rqueueQDetailService.getWaitingTasks();
    model.addAttribute("tasks", l.subList(1, l.size()));
    model.addAttribute("header", l.get(0));
  }

  @Override
  public void utility(Model model, String xForwardedPrefix) {
    addBasicDetails(model, xForwardedPrefix);
    addNavData(model, NavTab.UTILITY);
    model.addAttribute("title", "Utility");
    model.addAttribute("supportedDataType", DataType.getEnabledDataTypes());
  }
}
