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
import com.github.sonus21.rqueue.dao.RqueueQStatsDao;
import com.github.sonus21.rqueue.exception.UnknownSwitchCase;
import com.github.sonus21.rqueue.models.MinMax;
import com.github.sonus21.rqueue.models.aggregator.TasksStat;
import com.github.sonus21.rqueue.models.db.JobRunTime;
import com.github.sonus21.rqueue.models.db.QueueStatistics;
import com.github.sonus21.rqueue.models.enums.AggregationType;
import com.github.sonus21.rqueue.models.enums.ChartDataType;
import com.github.sonus21.rqueue.models.request.ChartDataRequest;
import com.github.sonus21.rqueue.models.response.ChartDataResponse;
import com.github.sonus21.rqueue.utils.Constants;
import com.github.sonus21.rqueue.utils.DateTimeUtils;
import com.github.sonus21.rqueue.utils.StringUtils;
import com.github.sonus21.rqueue.web.service.RqueueDashboardChartService;
import com.github.sonus21.rqueue.web.service.RqueueSystemManagerService;
import java.io.Serializable;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.stream.Collectors;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.util.CollectionUtils;
import reactor.core.publisher.Mono;

@Service
public class RqueueDashboardChartServiceImpl implements RqueueDashboardChartService {

  private final RqueueQStatsDao rqueueQStatsDao;
  private final RqueueConfig rqueueConfig;
  private final RqueueWebConfig rqueueWebConfig;
  private final RqueueSystemManagerService rqueueSystemManagerService;

  @Autowired
  public RqueueDashboardChartServiceImpl(
      RqueueQStatsDao rqueueQStatsDao,
      RqueueConfig rqueueConfig,
      RqueueWebConfig rqueueWebConfig,
      RqueueSystemManagerService rqueueSystemManagerService) {
    this.rqueueQStatsDao = rqueueQStatsDao;
    this.rqueueConfig = rqueueConfig;
    this.rqueueWebConfig = rqueueWebConfig;
    this.rqueueSystemManagerService = rqueueSystemManagerService;
  }

  private List<ChartDataType> getChartDataType(List<ChartDataType> chartDataTypes) {
    if (!CollectionUtils.isEmpty(chartDataTypes)) {
      return chartDataTypes;
    }
    return Arrays.stream(ChartDataType.values())
        .filter(ChartDataType::isUserView)
        .collect(Collectors.toList());
  }

  private void aggregateOneDataPoint(TasksStat stat, ChartDataType status, Object delta) {
    switch (status) {
      case SUCCESSFUL:
        stat.success += (long) delta;
        break;
      case MOVED_TO_DLQ:
        stat.movedToDlq += (long) delta;
        break;
      case DISCARDED:
        stat.discarded += (long) delta;
        break;
      case RETRIED:
        stat.retried += (long) delta;
        break;
      case EXECUTION:
        JobRunTime jobRunTime = (JobRunTime) delta;
        stat.minExecution = Math.min(stat.minExecution, jobRunTime.getMin());
        stat.maxExecution = Math.max(stat.maxExecution, jobRunTime.getMax());
        stat.jobCount += jobRunTime.getJobCount();
        stat.totalExecutionTime += jobRunTime.getTotalExecutionTime();
        break;
      default:
        throw new UnknownSwitchCase(status.name());
    }
  }

  @SuppressWarnings("unchecked")
  private void aggregateOneQuant(
      IdGenerator idGenerator,
      Map<Integer, TasksStat> idToTasksStat,
      Map dateToObject,
      LocalDate today,
      MinMax<Integer> minMax,
      ChartDataType status) {
    for (Object mapEntry : dateToObject.entrySet()) {
      Entry<String, Object> entry = (Entry<String, Object>) (mapEntry);
      String id = entry.getKey();
      int key = idGenerator.getId(id, today);
      if (key > minMax.getMax()) {
        minMax.setMax(key);
      }
      if (key < minMax.getMin()) {
        minMax.setMin(key);
      }
      TasksStat stat = idToTasksStat.getOrDefault(key, new TasksStat());
      aggregateOneDataPoint(stat, status, entry.getValue());
      idToTasksStat.put(key, stat);
    }
  }

  private Map<Integer, TasksStat> aggregateData(
      IdGenerator idGenerator,
      List<QueueStatistics> queueStatisticsList,
      List<ChartDataType> chartDataTypeList,
      int maxRequired) {
    MinMax<Integer> jobRunTime = new MinMax<>(Integer.MAX_VALUE, 0);
    LocalDate today = DateTimeUtils.today();
    Map<Integer, TasksStat> idToTasksStat = new HashMap<>();
    for (QueueStatistics queueStatistics : queueStatisticsList) {
      for (ChartDataType status : chartDataTypeList) {
        switch (status) {
          case RETRIED:
            aggregateOneQuant(
                idGenerator,
                idToTasksStat,
                queueStatistics.getTasksRetried(),
                today,
                jobRunTime,
                status);
            break;
          case DISCARDED:
            aggregateOneQuant(
                idGenerator,
                idToTasksStat,
                queueStatistics.getTasksDiscarded(),
                today,
                jobRunTime,
                status);

            break;
          case MOVED_TO_DLQ:
            aggregateOneQuant(
                idGenerator,
                idToTasksStat,
                queueStatistics.getTasksMovedToDeadLetter(),
                today,
                jobRunTime,
                status);
            break;
          case SUCCESSFUL:
            aggregateOneQuant(
                idGenerator,
                idToTasksStat,
                queueStatistics.getTasksSuccessful(),
                today,
                jobRunTime,
                status);
            break;
          case EXECUTION:
            aggregateOneQuant(
                idGenerator,
                idToTasksStat,
                queueStatistics.getJobRunTime(),
                today,
                jobRunTime,
                status);
            break;
          default:
            throw new UnknownSwitchCase(status.name());
        }
      }
    }

    if (!idToTasksStat.isEmpty()) {
      for (int i = 0; i < jobRunTime.getMin(); i++) {
        idToTasksStat.put(i, new TasksStat());
      }
    }
    if (jobRunTime.getMax() == 0 && idToTasksStat.isEmpty()) {
      idToTasksStat.put(0, new TasksStat());
    }
    for (int i = jobRunTime.getMax() + 1; i < maxRequired; i++) {
      idToTasksStat.put(i, new TasksStat());
    }
    return idToTasksStat;
  }

  private List<Serializable> getHeader(String title, List<ChartDataType> chartDataTypeList) {
    List<Serializable> row = new LinkedList<>();
    row.add(title);
    if (chartDataTypeList.size() == 1 && chartDataTypeList.get(0) == ChartDataType.EXECUTION) {
      row.add("Minimum execution time");
      row.add("Maximum execution time");
      row.add("Average execution time");
    } else {
      for (ChartDataType status : chartDataTypeList) {
        row.add(status.getDescription());
      }
    }

    return row;
  }

  private List<Serializable> createRow(
      Integer id, TasksStat tasksStat, List<ChartDataType> chartDataTypeList) {
    List<Serializable> row = new LinkedList<>();
    row.add(String.valueOf(id + 1));
    for (ChartDataType status : chartDataTypeList) {
      switch (status) {
        case RETRIED:
          row.add(tasksStat.retried);
          break;
        case DISCARDED:
          row.add(tasksStat.discarded);
          break;
        case MOVED_TO_DLQ:
          row.add(tasksStat.movedToDlq);
          break;
        case SUCCESSFUL:
          row.add(tasksStat.success);
          break;
        case EXECUTION:
          long minExecution = tasksStat.minExecution;
          if (minExecution == Long.MAX_VALUE) {
            minExecution = 0;
          }
          row.add(minExecution);
          row.add(tasksStat.maxExecution);
          long average = 0;
          if (tasksStat.jobCount != 0) {
            average = tasksStat.totalExecutionTime / tasksStat.jobCount;
          }
          row.add(average);
          break;
        default:
          throw new UnknownSwitchCase(status.name());
      }
    }
    return row;
  }

  private List<List<Serializable>> createChartData(
      String title, List<ChartDataType> chartDataTypeList, Map<Integer, TasksStat> idToTasksStat) {
    List<Entry<Integer, TasksStat>> entries =
        idToTasksStat.entrySet().stream()
            .sorted((o1, o2) -> o2.getKey() - o1.getKey())
            .collect(Collectors.toList());
    List<List<Serializable>> rows = new ArrayList<>();
    rows.add(getHeader(title, chartDataTypeList));
    for (Entry<Integer, TasksStat> entry : entries) {
      rows.add(createRow(entry.getKey(), entry.getValue(), chartDataTypeList));
    }
    return rows;
  }

  private List<List<Serializable>> aggregateMonthly(
      int numberOfDays,
      List<QueueStatistics> queueStatisticsList,
      List<ChartDataType> chartDataTypeList) {
    Map<Integer, TasksStat> monthToChartDataType =
        aggregateData(
            (date, today) ->
                (int) Math.floor((today.toEpochDay() - LocalDate.parse(date).toEpochDay()) / 30.0f),
            queueStatisticsList,
            chartDataTypeList,
            getCount(numberOfDays, Constants.DAYS_IN_A_MONTH));
    return createChartData("Monthly", chartDataTypeList, monthToChartDataType);
  }

  private List<List<Serializable>> aggregateDaily(
      int numberOfDays,
      List<QueueStatistics> queueStatisticsList,
      List<ChartDataType> chartDataTypes) {
    Map<Integer, TasksStat> dayToChartDataType =
        aggregateData(
            (date, today) -> (int) (today.toEpochDay() - LocalDate.parse(date).toEpochDay()),
            queueStatisticsList,
            chartDataTypes,
            getCount(numberOfDays, 1));
    return createChartData("Daily", chartDataTypes, dayToChartDataType);
  }

  private int getCount(int numberOfDays, int factor) {
    if (factor == 1) {
      return numberOfDays;
    }
    return (int) Math.ceil(numberOfDays * 1.0f / factor);
  }

  private List<List<Serializable>> aggregateWeekly(
      int numberOfDays,
      List<QueueStatistics> queueStatisticsList,
      List<ChartDataType> chartDataTypeList) {
    Map<Integer, TasksStat> weekToChartDataType =
        aggregateData(
            (date, today) ->
                (int) Math.floor((today.toEpochDay() - LocalDate.parse(date).toEpochDay()) / 7.0f),
            queueStatisticsList,
            chartDataTypeList,
            getCount(numberOfDays, Constants.DAYS_IN_A_WEEK));
    return createChartData("Weekly", chartDataTypeList, weekToChartDataType);
  }

  private List<List<Serializable>> getChartData(
      int numberOfDays,
      Collection<String> ids,
      AggregationType type,
      List<ChartDataType> chartDataTypes) {
    List<QueueStatistics> queueStatistics = rqueueQStatsDao.findAll(ids);
    if (queueStatistics == null) {
      queueStatistics = new ArrayList<>();
    }
    switch (type) {
      case DAILY:
        return aggregateDaily(numberOfDays, queueStatistics, getChartDataType(chartDataTypes));
      case WEEKLY:
        return aggregateWeekly(numberOfDays, queueStatistics, getChartDataType(chartDataTypes));
      case MONTHLY:
        return aggregateMonthly(numberOfDays, queueStatistics, getChartDataType(chartDataTypes));
      default:
        throw new UnknownSwitchCase(type.name());
    }
  }

  private ChartDataResponse getQueueStats(ChartDataRequest chartDataRequest) {
    Collection<String> ids = getQueueStatsId(chartDataRequest);
    List<List<Serializable>> rows =
        getChartData(
            chartDataRequest.numberOfDays(rqueueWebConfig),
            ids,
            chartDataRequest.getAggregationType(),
            chartDataRequest.getDateTypes());
    ChartDataResponse response = new ChartDataResponse();
    response.setData(rows);
    response.setHTitle(chartDataRequest.getAggregationType().getDescription());
    response.setVTitle("N");
    if (StringUtils.isEmpty(chartDataRequest.getQueue())) {
      response.setTitle("Task Status");
    } else {
      response.setTitle("Queue[" + chartDataRequest.getQueue() + "]");
    }
    return response;
  }

  private Collection<String> getQueueStatsId(ChartDataRequest chartDataRequest) {
    Collection<String> ids = Collections.emptyList();
    if (!StringUtils.isEmpty(chartDataRequest.getQueue())) {
      ids = Collections.singleton(rqueueConfig.getQueueStatisticsKey(chartDataRequest.getQueue()));
    } else {
      List<String> queues = rqueueSystemManagerService.getQueues();
      if (!CollectionUtils.isEmpty(queues)) {
        ids = queues.stream().map(rqueueConfig::getQueueStatisticsKey).collect(Collectors.toList());
      }
    }
    return ids;
  }

  private ChartDataResponse getQueueLatency(ChartDataRequest chartDataRequest) {
    Collection<String> ids = getQueueStatsId(chartDataRequest);
    List<List<Serializable>> rows =
        getChartData(
            chartDataRequest.numberOfDays(rqueueWebConfig),
            ids,
            chartDataRequest.getAggregationType(),
            Collections.singletonList(ChartDataType.EXECUTION));
    ChartDataResponse response = new ChartDataResponse();
    response.setData(rows);
    response.setHTitle(chartDataRequest.getAggregationType().getDescription());
    response.setVTitle("Milliseconds");
    if (!StringUtils.isEmpty(chartDataRequest.getQueue())) {
      response.setTitle("Latency [" + chartDataRequest.getQueue() + "]");
    } else {
      response.setTitle("Overall Latency");
    }
    return response;
  }

  @Override
  public ChartDataResponse getDashboardChartData(ChartDataRequest chartDataRequest) {
    ChartDataResponse chartDataResponse = chartDataRequest.validate();
    if (chartDataResponse != null) {
      return chartDataResponse;
    }
    switch (chartDataRequest.getType()) {
      case LATENCY:
        return getQueueLatency(chartDataRequest);
      case STATS:
        return getQueueStats(chartDataRequest);
      default:
        throw new UnknownSwitchCase(chartDataRequest.getType().name());
    }
  }

  @Override
  public Mono<ChartDataResponse> getReactiveDashBoardData(ChartDataRequest chartDataRequest) {
    ChartDataResponse response = getDashboardChartData(chartDataRequest);
    return Mono.just(response);
  }

  private interface IdGenerator {

    int getId(String date, LocalDate today);
  }
}
