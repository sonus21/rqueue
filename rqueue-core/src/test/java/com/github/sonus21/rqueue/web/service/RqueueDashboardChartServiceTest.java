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

package com.github.sonus21.rqueue.web.service;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.anyCollection;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import com.github.sonus21.TestBase;
import com.github.sonus21.rqueue.CoreUnitTest;
import com.github.sonus21.rqueue.config.RqueueConfig;
import com.github.sonus21.rqueue.config.RqueueWebConfig;
import com.github.sonus21.rqueue.dao.RqueueQStatsDao;
import com.github.sonus21.rqueue.models.db.JobRunTime;
import com.github.sonus21.rqueue.models.db.QueueStatistics;
import com.github.sonus21.rqueue.models.db.QueueStatisticsTest;
import com.github.sonus21.rqueue.models.enums.AggregationType;
import com.github.sonus21.rqueue.models.enums.ChartDataType;
import com.github.sonus21.rqueue.models.enums.ChartType;
import com.github.sonus21.rqueue.models.request.ChartDataRequest;
import com.github.sonus21.rqueue.models.response.ChartDataResponse;
import com.github.sonus21.rqueue.utils.DateTimeUtils;
import com.github.sonus21.rqueue.web.service.impl.RqueueDashboardChartServiceImpl;
import java.io.Serializable;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

@Slf4j
@CoreUnitTest
class RqueueDashboardChartServiceTest extends TestBase {

  private final List<String> queues = new ArrayList<>();
  @Mock
  private RqueueQStatsDao rqueueQStatsDao;
  @Mock
  private RqueueWebConfig rqueueWebConfig;
  @Mock
  private RqueueConfig rqueueConfig;
  @Mock
  private RqueueSystemManagerService rqueueSystemManagerService;
  private RqueueDashboardChartService rqueueDashboardChartService;

  @BeforeEach
  public void init() {
    MockitoAnnotations.openMocks(this);
    rqueueDashboardChartService =
        new RqueueDashboardChartServiceImpl(
            rqueueQStatsDao, rqueueConfig, rqueueWebConfig, rqueueSystemManagerService);
    doReturn(180).when(rqueueWebConfig).getHistoryDay();
    doAnswer(
        invocation -> {
          String name = invocation.getArgument(0);
          return "__rq::q-stat::" + name;
        })
        .when(rqueueConfig)
        .getQueueStatisticsKey(anyString());
    queues.clear();
    queues.add("job");
    queues.add("notification");
    queues.add("email");
  }

  private void verifyLatencyChartHeaders(ChartDataRequest chartDataRequest, int dpoint) {
    ChartDataResponse response =
        rqueueDashboardChartService.getDashboardChartData(chartDataRequest);
    assertEquals("Milliseconds", response.getVTitle());
    assertEquals(chartDataRequest.getAggregationType().getDescription(), response.getHTitle());
    if (chartDataRequest.getQueue() == null) {
      assertEquals("Overall Latency", response.getTitle());
    } else {
      assertEquals("Latency [test-queue]", response.getTitle());
    }
    assertEquals(dpoint, response.getData().size());
    assertEquals(4, response.getData().get(0).size());

    List<String> list = new ArrayList<>();
    list.add(chartDataRequest.getAggregationType().getDescription());
    list.add("Minimum execution time");
    list.add("Maximum execution time");
    list.add("Average execution time");
    assertEquals(list, response.getData().get(0));
    verifyDefaultValue(response, dpoint, list.size());
  }

  @Test
  void latencyChartHeaders() {
    doReturn(queues).when(rqueueSystemManagerService).getQueues();
    ChartDataRequest chartDataRequest = new ChartDataRequest();
    chartDataRequest.setType(ChartType.LATENCY);
    chartDataRequest.setAggregationType(AggregationType.DAILY);
    verifyLatencyChartHeaders(chartDataRequest, 181);
    chartDataRequest.setQueue("test-queue");
    verifyLatencyChartHeaders(chartDataRequest, 181);
    List<ChartDataType> d = new ArrayList<>();
    d.add(ChartDataType.DISCARDED);
    d.add(ChartDataType.MOVED_TO_DLQ);
    chartDataRequest.setDateTypes(d);
    verifyLatencyChartHeaders(chartDataRequest, 181);

    chartDataRequest.setQueue(null);
    chartDataRequest.setAggregationType(AggregationType.WEEKLY);
    verifyLatencyChartHeaders(chartDataRequest, 27);
    chartDataRequest.setQueue("test-queue");
    verifyLatencyChartHeaders(chartDataRequest, 27);

    chartDataRequest.setQueue(null);
    chartDataRequest.setAggregationType(AggregationType.MONTHLY);
    verifyLatencyChartHeaders(chartDataRequest, 7);
    chartDataRequest.setQueue("test-queue");
    verifyLatencyChartHeaders(chartDataRequest, 7);
    verify(rqueueQStatsDao, times(7)).findAll(anyCollection());
    verify(rqueueSystemManagerService, times(3)).getQueues();
  }

  private void verifyDefaultValue(ChartDataResponse response, int rows, int columns) {
    for (int i = 1; i < rows; i++) {
      List<Serializable> row = response.getData().get(i);
      for (int j = 0; j < columns; j++) {
        if (j == 0) {
          assertEquals(String.valueOf(rows - i), row.get(j));
        } else {
          assertEquals(0L, row.get(j));
        }
      }
    }
  }

  private void verifyStatsChartHeaders(ChartDataRequest chartDataRequest, int dpoint) {
    ChartDataResponse response =
        rqueueDashboardChartService.getDashboardChartData(chartDataRequest);
    assertEquals("N", response.getVTitle());
    assertEquals(chartDataRequest.getAggregationType().getDescription(), response.getHTitle());
    if (chartDataRequest.getQueue() == null) {
      assertEquals("Task Status", response.getTitle());
    } else {
      assertEquals("Queue[test-queue]", response.getTitle());
    }
    assertEquals(dpoint, response.getData().size());
    List<String> list = new ArrayList<>();
    list.add(chartDataRequest.getAggregationType().getDescription());
    if (chartDataRequest.getDateTypes() == null) {
      assertEquals(5, response.getData().get(0).size());
      list.add(ChartDataType.SUCCESSFUL.getDescription());
      list.add(ChartDataType.DISCARDED.getDescription());
      list.add(ChartDataType.MOVED_TO_DLQ.getDescription());
      list.add(ChartDataType.RETRIED.getDescription());
    } else {
      assertEquals(1 + chartDataRequest.getDateTypes().size(), response.getData().get(0).size());
      for (ChartDataType d : chartDataRequest.getDateTypes()) {
        list.add(d.getDescription());
      }
    }
    assertEquals(list, response.getData().get(0));
    verifyDefaultValue(response, dpoint, list.size());
  }

  @Test
  void statsChartHeaders() {
    doReturn(queues).when(rqueueSystemManagerService).getQueues();
    ChartDataRequest chartDataRequest = new ChartDataRequest();
    chartDataRequest.setType(ChartType.STATS);
    chartDataRequest.setAggregationType(AggregationType.DAILY);
    verifyStatsChartHeaders(chartDataRequest, 181);
    chartDataRequest.setQueue("test-queue");
    verifyStatsChartHeaders(chartDataRequest, 181);
    List<ChartDataType> d = new ArrayList<>();
    d.add(ChartDataType.DISCARDED);
    d.add(ChartDataType.MOVED_TO_DLQ);
    chartDataRequest.setDateTypes(d);
    verifyStatsChartHeaders(chartDataRequest, 181);

    chartDataRequest.setQueue(null);
    chartDataRequest.setAggregationType(AggregationType.WEEKLY);
    verifyStatsChartHeaders(chartDataRequest, 27);
    chartDataRequest.setQueue("test-queue");
    verifyStatsChartHeaders(chartDataRequest, 27);

    chartDataRequest.setQueue(null);
    chartDataRequest.setAggregationType(AggregationType.MONTHLY);
    verifyStatsChartHeaders(chartDataRequest, 7);
    chartDataRequest.setQueue("test-queue");
    verifyStatsChartHeaders(chartDataRequest, 7);
    verify(rqueueQStatsDao, times(7)).findAll(anyCollection());
    verify(rqueueSystemManagerService, times(3)).getQueues();
  }

  @Test
  void getDashboardChartDataLatencyDaily() {
    LocalDateTime localDateTime = LocalDateTime.now();
    if (localDateTime.getHour() > 22) {
      log.info("test cannot be run at this time");
      return;
    }
    String id = "__rq::q-stat::job";
    QueueStatistics queueStatistics = new QueueStatistics(id);
    LocalDate localDate = DateTimeUtils.today();
    for (int i = 0; i < rqueueWebConfig.getHistoryDay(); i++) {
      QueueStatisticsTest.addData(queueStatistics, localDate, i);
    }
    doReturn(Collections.singletonList(queueStatistics))
        .when(rqueueQStatsDao)
        .findAll(Collections.singleton(id));
    ChartDataRequest chartDataRequest = new ChartDataRequest();
    chartDataRequest.setType(ChartType.LATENCY);
    chartDataRequest.setAggregationType(AggregationType.DAILY);
    chartDataRequest.setQueue("job");
    ChartDataResponse response =
        rqueueDashboardChartService.getDashboardChartData(chartDataRequest);
    assertEquals(rqueueWebConfig.getHistoryDay() + 1, response.getData().size());
    localDate = DateTimeUtils.today();
    for (int i = 1; i <= rqueueWebConfig.getHistoryDay(); i++) {
      LocalDate date = localDate.plusDays(-rqueueWebConfig.getHistoryDay() + i);
      JobRunTime jobRunTime = queueStatistics.jobRunTime(date.toString());
      List<Serializable> row = response.getData().get(i);
      long average = 0;
      if (jobRunTime.getJobCount() != 0) {
        average = jobRunTime.getTotalExecutionTime() / jobRunTime.getJobCount();
      }
      assertEquals(jobRunTime.getMin(), row.get(1));
      assertEquals(jobRunTime.getMax(), row.get(2));
      assertEquals(average, row.get(3));
    }
  }

  @Test
  void getDashboardChartDataStatsDaily() {
    LocalDateTime localDateTime = LocalDateTime.now();
    if (localDateTime.getHour() > 22) {
      log.info("test cannot be run at this time");
      return;
    }
    String id = "__rq::q-stat::job";
    QueueStatistics queueStatistics = new QueueStatistics(id);
    LocalDate localDate = DateTimeUtils.today();
    for (int i = 0; i < rqueueWebConfig.getHistoryDay(); i++) {
      QueueStatisticsTest.addData(queueStatistics, localDate, i);
    }
    doReturn(Collections.singletonList(queueStatistics))
        .when(rqueueQStatsDao)
        .findAll(Collections.singleton(id));
    ChartDataRequest chartDataRequest = new ChartDataRequest();
    chartDataRequest.setType(ChartType.STATS);
    chartDataRequest.setAggregationType(AggregationType.DAILY);
    chartDataRequest.setQueue("job");
    ChartDataResponse response =
        rqueueDashboardChartService.getDashboardChartData(chartDataRequest);
    assertEquals(rqueueWebConfig.getHistoryDay() + 1, response.getData().size());
    localDate = DateTimeUtils.today();
    for (int i = 1; i <= rqueueWebConfig.getHistoryDay(); i++) {
      LocalDate date = localDate.plusDays(-rqueueWebConfig.getHistoryDay() + i);
      long discarded = queueStatistics.tasksDiscarded(date.toString());
      long successful = queueStatistics.tasksSuccessful(date.toString());
      long dlq = queueStatistics.tasksMovedToDeadLetter(date.toString());
      long retries = queueStatistics.tasksRetried(date.toString());
      List<Serializable> row = response.getData().get(i);
      assertEquals(successful, row.get(1));
      assertEquals(discarded, row.get(2));
      assertEquals(dlq, row.get(3));
      assertEquals(retries, row.get(4));
    }
  }

  @Test
  void getDashboardChartDataStatsDailyMissingDays() {
    LocalDateTime localDateTime = LocalDateTime.now();
    if (localDateTime.getHour() > 22) {
      log.info("test cannot be run at this time");
      return;
    }
    String id = "__rq::q-stat::job";
    QueueStatistics queueStatistics = new QueueStatistics(id);
    LocalDate localDate = DateTimeUtils.today();
    for (int i = 10; i < rqueueWebConfig.getHistoryDay() - 10; i++) {
      QueueStatisticsTest.addData(queueStatistics, localDate, i);
    }
    doReturn(Collections.singletonList(queueStatistics))
        .when(rqueueQStatsDao)
        .findAll(Collections.singleton(id));
    ChartDataRequest chartDataRequest = new ChartDataRequest();
    chartDataRequest.setType(ChartType.STATS);
    chartDataRequest.setAggregationType(AggregationType.DAILY);
    chartDataRequest.setQueue("job");
    ChartDataResponse response =
        rqueueDashboardChartService.getDashboardChartData(chartDataRequest);
    assertEquals(rqueueWebConfig.getHistoryDay() + 1, response.getData().size());
    localDate = DateTimeUtils.today();
    for (int i = 1; i <= rqueueWebConfig.getHistoryDay(); i++) {
      LocalDate date = localDate.plusDays(-rqueueWebConfig.getHistoryDay() + i);
      long discarded = queueStatistics.tasksDiscarded(date.toString());
      long successful = queueStatistics.tasksSuccessful(date.toString());
      long dlq = queueStatistics.tasksMovedToDeadLetter(date.toString());
      long retries = queueStatistics.tasksRetried(date.toString());
      List<Serializable> row = response.getData().get(i);
      assertEquals(successful, row.get(1));
      assertEquals(discarded, row.get(2));
      assertEquals(dlq, row.get(3));
      assertEquals(retries, row.get(4));
    }
  }
}
