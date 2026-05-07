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

package com.github.sonus21.rqueue.web.service;

import com.github.sonus21.rqueue.config.RqueueConfig;
import com.github.sonus21.rqueue.config.RqueueWebConfig;
import com.github.sonus21.rqueue.core.spi.Capabilities;
import com.github.sonus21.rqueue.core.spi.MessageBroker;
import com.github.sonus21.rqueue.models.Pair;
import com.github.sonus21.rqueue.models.db.QueueConfig;
import com.github.sonus21.rqueue.models.enums.AggregationType;
import com.github.sonus21.rqueue.models.enums.ChartDataType;
import com.github.sonus21.rqueue.models.enums.DataType;
import com.github.sonus21.rqueue.models.enums.NavTab;
import com.github.sonus21.rqueue.models.registry.RqueueWorkerPollerView;
import com.github.sonus21.rqueue.models.registry.RqueueWorkerView;
import com.github.sonus21.rqueue.models.response.RedisDataDetail;
import com.github.sonus21.rqueue.service.RqueueUtilityService;
import com.github.sonus21.rqueue.utils.DateTimeUtils;
import com.github.sonus21.rqueue.web.RqueueQDetailService;
import com.github.sonus21.rqueue.web.RqueueSystemManagerService;
import com.github.sonus21.rqueue.web.RqueueViewControllerService;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
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

  /**
   * Optional broker SPI. When set (non-Redis backend), {@link #addBasicDetails(Model, String)}
   * propagates {@link Capabilities} flags to every view template so the navigation, charts, and
   * other panels can hide unsupported sections globally.
   */
  private MessageBroker messageBroker;

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

  @Autowired(required = false)
  public void setMessageBroker(MessageBroker messageBroker) {
    this.messageBroker = messageBroker;
  }

  private void addNavData(Model model, NavTab tab) {
    for (NavTab navTab : NavTab.values()) {
      String name = navTab.name().toLowerCase() + "Active";
      model.addAttribute(name, tab == navTab);
    }
  }

  /**
   * Resolved capabilities for the active broker. Defaults to {@link Capabilities#REDIS_DEFAULTS}
   * (everything supported) so the legacy no-broker path keeps the historical UI.
   */
  private Capabilities capabilities() {
    return messageBroker != null ? messageBroker.capabilities() : Capabilities.REDIS_DEFAULTS;
  }

  private void addBasicDetails(Model model, String xForwardedPrefix) {
    Pair<String, String> releaseAndVersion = rqueueUtilityService.getLatestVersion();
    model.addAttribute("releaseLink", releaseAndVersion.getFirst());
    model.addAttribute("latestVersion", releaseAndVersion.getSecond());
    model.addAttribute("time", DateTimeUtils.currentTimeFormatted());
    model.addAttribute("timeInMilli", System.currentTimeMillis());
    model.addAttribute("version", rqueueConfig.getLibVersion());
    model.addAttribute("urlPrefix", rqueueWebConfig.getUrlPrefix(xForwardedPrefix));
    // Capability-driven UI hide flags. Templates default to "show" when these are absent /
    // false, matching the historical Redis behavior.
    Capabilities caps = capabilities();
    model.addAttribute("hideScheduledPanel", !caps.supportsScheduledIntrospection());
    model.addAttribute("hideRunningPanel", !caps.usesPrimaryHandlerDispatch());
    model.addAttribute("hideCronJobs", !caps.supportsCronJobs());
    model.addAttribute("hideExploreData", !caps.supportsViewData());
    model.addAttribute("hideMoveMessages", !caps.supportsMoveMessage());
    // Charts always render; NATS deployments will show empty until counters accumulate.
    model.addAttribute("hideCharts", false);
    model.addAttribute("storageKicker", rqueueQDetailService.storageKicker());
    model.addAttribute("storageDescription", rqueueQDetailService.storageDescription());
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

  private <T> List<T> getPage(List<T> items, int pageNumber, int pageSize) {
    if (items.isEmpty()) {
      return items;
    }
    int page = Math.max(1, pageNumber);
    int start = (page - 1) * pageSize;
    if (start >= items.size()) {
      start = ((items.size() - 1) / pageSize) * pageSize;
    }
    int end = Math.min(start + pageSize, items.size());
    return items.subList(start, end);
  }

  @Override
  public void queues(Model model, String xForwardedPrefix, int pageNumber) {
    addBasicDetails(model, xForwardedPrefix);
    addNavData(model, NavTab.QUEUES);
    model.addAttribute("title", "Queues");
    List<QueueConfig> allQueueConfigs = rqueueSystemManagerService.getSortedQueueConfigs();
    int pageSize = Math.max(1, rqueueWebConfig.getQueuePageSize());
    int totalPages = Math.max(1, (allQueueConfigs.size() + pageSize - 1) / pageSize);
    int currentPage = Math.max(1, Math.min(pageNumber, totalPages));
    List<QueueConfig> queueConfigs = getPage(allQueueConfigs, currentPage, pageSize);
    List<Entry<String, List<Entry<NavTab, RedisDataDetail>>>> queueNameConfigs = new ArrayList<>(
        rqueueQDetailService.getQueueDataStructureDetails(queueConfigs).entrySet());
    queueNameConfigs.sort(Entry.comparingByKey());
    model.addAttribute("queues", queueConfigs);
    model.addAttribute("queueConfigs", queueNameConfigs);
    model.addAttribute("storageKicker", rqueueQDetailService.storageKicker());
    model.addAttribute("storageDescription", rqueueQDetailService.storageDescription());
    model.addAttribute("currentPage", currentPage);
    model.addAttribute("totalPages", totalPages);
    model.addAttribute("hasPreviousPage", currentPage > 1);
    model.addAttribute("hasNextPage", currentPage < totalPages);
    model.addAttribute("previousPage", Math.max(1, currentPage - 1));
    model.addAttribute("nextPage", Math.min(totalPages, currentPage + 1));
    model.addAttribute("queuePageSize", pageSize);
    model.addAttribute("totalQueueCount", allQueueConfigs.size());
  }

  @Override
  public void workers(Model model, String xForwardedPrefix, int pageNumber) {
    addBasicDetails(model, xForwardedPrefix);
    addNavData(model, NavTab.WORKERS);
    model.addAttribute("title", "Workers");
    List<QueueConfig> queueConfigs = rqueueSystemManagerService.getSortedQueueConfigs();
    Map<String, RqueueWorkerView> workerIdToView = new LinkedHashMap<>();
    for (QueueConfig queueConfig : queueConfigs) {
      List<RqueueWorkerPollerView> queueWorkers =
          rqueueQDetailService.getQueueWorkers(queueConfig.getName());
      for (RqueueWorkerPollerView queueWorker : queueWorkers) {
        RqueueWorkerView workerView = workerIdToView.getOrDefault(
            queueWorker.getWorkerId(),
            RqueueWorkerView.builder()
                .workerId(queueWorker.getWorkerId())
                .host(queueWorker.getHost())
                .pid(queueWorker.getPid())
                .lastPollAt(queueWorker.getLastPollAt())
                .lastPollAge(queueWorker.getLastPollAge())
                .build());
        workerView.setHost(queueWorker.getHost());
        workerView.setPid(queueWorker.getPid());
        if (queueWorker.getLastPollAt() > workerView.getLastPollAt()) {
          workerView.setLastPollAt(queueWorker.getLastPollAt());
          workerView.setLastPollAge(queueWorker.getLastPollAge());
        }
        if ("ACTIVE".equals(queueWorker.getStatus())) {
          workerView.setActiveQueues(workerView.getActiveQueues() + 1);
        } else {
          workerView.setStaleQueues(workerView.getStaleQueues() + 1);
        }
        workerView.getPollers().add(queueWorker);
        workerIdToView.put(queueWorker.getWorkerId(), workerView);
      }
    }
    List<RqueueWorkerView> allWorkers = new ArrayList<>(workerIdToView.values());
    allWorkers.forEach(e -> {
      e.getPollers().sort(Comparator.comparing(RqueueWorkerPollerView::getQueue));
      int recentCapacityExhaustedQueues = 0;
      long recentThreshold =
          2L * rqueueConfig.getWorkerRegistryQueueHeartbeatInterval().toMillis();
      for (RqueueWorkerPollerView poller : e.getPollers()) {
        Long lastCapacityExhaustedAt = poller.getLastCapacityExhaustedAt();
        if (lastCapacityExhaustedAt != null
            && System.currentTimeMillis() - lastCapacityExhaustedAt <= recentThreshold) {
          recentCapacityExhaustedQueues += 1;
        }
      }
      e.setRecentCapacityExhaustedQueues(recentCapacityExhaustedQueues);
    });
    allWorkers.sort(Comparator.comparingLong(RqueueWorkerView::getLastPollAt).reversed());
    int pageSize = Math.max(1, rqueueWebConfig.getWorkerPageSize());
    int totalPages = Math.max(1, (allWorkers.size() + pageSize - 1) / pageSize);
    int currentPage = Math.max(1, Math.min(pageNumber, totalPages));
    List<RqueueWorkerView> workers = getPage(allWorkers, currentPage, pageSize);
    model.addAttribute("workers", workers);
    model.addAttribute("currentPage", currentPage);
    model.addAttribute("totalPages", totalPages);
    model.addAttribute("hasPreviousPage", currentPage > 1);
    model.addAttribute("hasNextPage", currentPage < totalPages);
    model.addAttribute("previousPage", Math.max(1, currentPage - 1));
    model.addAttribute("nextPage", Math.min(totalPages, currentPage + 1));
    model.addAttribute("workerPageSize", pageSize);
    model.addAttribute("totalWorkerCount", allWorkers.size());
  }

  @Override
  public void queueDetail(Model model, String xForwardedPrefix, String queueName) {
    QueueConfig queueConfig = rqueueSystemManagerService.getQueueConfig(queueName);
    List<NavTab> queueActions = rqueueQDetailService.getNavTabs(queueConfig);
    List<Entry<NavTab, RedisDataDetail>> queueRedisDataDetail =
        rqueueQDetailService.getQueueDataStructureDetail(queueConfig);
    List<RqueueWorkerPollerView> queueWorkers = rqueueQDetailService.getQueueWorkers(queueName);
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
    // New per-subscriber + terminal-storage rows; the template renders these in place of the
    // legacy data-structure table when present.
    model.addAttribute("subscribers", rqueueQDetailService.getSubscriberRows(queueConfig));
    model.addAttribute("terminalRows", rqueueQDetailService.getTerminalRows(queueConfig));
    model.addAttribute("config", queueConfig);
    model.addAttribute("workerRegistryEnabled", rqueueConfig.isWorkerRegistryEnabled());
    model.addAttribute("queueWorkers", queueWorkers);
    model.addAttribute(
        "activeQueueWorkers",
        queueWorkers.stream().filter(e -> "ACTIVE".equals(e.getStatus())).count());
    model.addAttribute(
        "staleQueueWorkers",
        queueWorkers.stream().filter(e -> "STALE".equals(e.getStatus())).count());
    long recentThreshold =
        2L * rqueueConfig.getWorkerRegistryQueueHeartbeatInterval().toMillis();
    model.addAttribute(
        "queueWorkerRecentCapacityExhausted",
        queueWorkers.stream()
            .filter(e -> e.getLastCapacityExhaustedAt() != null
                && System.currentTimeMillis() - e.getLastCapacityExhaustedAt() <= recentThreshold)
            .count());
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
