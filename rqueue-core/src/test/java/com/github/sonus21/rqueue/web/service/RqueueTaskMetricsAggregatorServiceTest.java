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
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;

import com.github.sonus21.TestBase;
import com.github.sonus21.rqueue.CoreUnitTest;
import com.github.sonus21.rqueue.common.RqueueLockManager;
import com.github.sonus21.rqueue.config.RqueueConfig;
import com.github.sonus21.rqueue.config.RqueueWebConfig;
import com.github.sonus21.rqueue.core.Job;
import com.github.sonus21.rqueue.core.RqueueMessage;
import com.github.sonus21.rqueue.core.RqueueMessageTemplate;
import com.github.sonus21.rqueue.dao.RqueueJobDao;
import com.github.sonus21.rqueue.dao.RqueueQStatsDao;
import com.github.sonus21.rqueue.exception.TimedOutException;
import com.github.sonus21.rqueue.listener.JobImpl;
import com.github.sonus21.rqueue.listener.QueueDetail;
import com.github.sonus21.rqueue.models.aggregator.TasksStat;
import com.github.sonus21.rqueue.models.db.MessageMetadata;
import com.github.sonus21.rqueue.models.db.QueueStatistics;
import com.github.sonus21.rqueue.models.db.QueueStatisticsTest;
import com.github.sonus21.rqueue.models.enums.MessageStatus;
import com.github.sonus21.rqueue.models.event.RqueueExecutionEvent;
import com.github.sonus21.rqueue.utils.Constants;
import com.github.sonus21.rqueue.utils.DateTimeUtils;
import com.github.sonus21.rqueue.utils.TestUtils;
import com.github.sonus21.rqueue.utils.TimeoutUtils;
import java.time.Duration;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

@Slf4j
@CoreUnitTest
class RqueueTaskMetricsAggregatorServiceTest extends TestBase {

  private final String queueName = "test-queue";
  @Mock
  private RqueueQStatsDao rqueueQStatsDao;
  @Mock
  private RqueueWebConfig rqueueWebConfig;
  @Mock
  private RqueueLockManager rqueueLockManager;
  @Mock
  private RqueueConfig rqueueConfig;
  @Mock
  private RqueueMessageMetadataService rqueueMessageMetadataService;
  @Mock
  private RqueueJobDao rqueueJobDao;
  @Mock
  private RqueueMessageTemplate rqueueMessageTemplate;
  private RqueueJobMetricsAggregatorService rqueueJobMetricsAggregatorService;

  @BeforeEach
  public void initService() throws IllegalAccessException {
    MockitoAnnotations.openMocks(this);
    rqueueJobMetricsAggregatorService =
        new RqueueJobMetricsAggregatorService(
            rqueueConfig, rqueueWebConfig, rqueueLockManager, rqueueQStatsDao);
    doReturn(true).when(rqueueWebConfig).isCollectListenerStats();
    doReturn(1).when(rqueueWebConfig).getStatsAggregatorThreadCount();
    doReturn(100).when(rqueueWebConfig).getAggregateEventWaitTimeInSecond();
    doReturn(100).when(rqueueWebConfig).getAggregateShutdownWaitTime();
    doReturn(180).when(rqueueWebConfig).getHistoryDay();
    doReturn(500).when(rqueueWebConfig).getAggregateEventCount();
    this.rqueueJobMetricsAggregatorService.start();
    assertNotNull(
        FieldUtils.readField(this.rqueueJobMetricsAggregatorService, "queueNameToEvents", true));
    assertNotNull(FieldUtils.readField(this.rqueueJobMetricsAggregatorService, "queue", true));
    assertNotNull(
        FieldUtils.readField(this.rqueueJobMetricsAggregatorService, "taskExecutor", true));
  }

  private RqueueExecutionEvent generateTaskEventWithStatus(MessageStatus status) {
    double r = Math.random();
    RqueueMessage rqueueMessage =
        RqueueMessage.builder()
            .queueName("test-queue")
            .message("test")
            .processAt(System.currentTimeMillis())
            .queuedTime(System.nanoTime())
            .build();
    MessageMetadata messageMetadata = new MessageMetadata(rqueueMessage, status);
    messageMetadata.setTotalExecutionTime(10 + (long) r * 10000);
    rqueueMessage.setFailureCount((int) r * 10);
    QueueDetail queueDetail = TestUtils.createQueueDetail(queueName);
    Job job =
        new JobImpl(
            rqueueConfig,
            rqueueMessageMetadataService,
            rqueueJobDao,
            rqueueMessageTemplate,
            rqueueLockManager,
            queueDetail,
            messageMetadata,
            rqueueMessage,
            null,
            null);
    return new RqueueExecutionEvent(job);
  }

  private RqueueExecutionEvent generateTaskEvent() {
    double r = Math.random();
    MessageStatus messageStatus;
    if (r < 0.3) {
      messageStatus = MessageStatus.SUCCESSFUL;
    } else if (r < 0.6) {
      messageStatus = MessageStatus.DISCARDED;
    } else {
      messageStatus = MessageStatus.MOVED_TO_DLQ;
    }
    return generateTaskEventWithStatus(messageStatus);
  }

  private void addEvent(RqueueExecutionEvent event, TasksStat stats, boolean updateTaskStat) {
    rqueueJobMetricsAggregatorService.onApplicationEvent(event);
    if (!updateTaskStat) {
      return;
    }
    switch (event.getJob().getMessageMetadata().getStatus()) {
      case DISCARDED:
        stats.discarded += 1;
        break;
      case SUCCESSFUL:
        stats.success += 1;
        break;
      case MOVED_TO_DLQ:
        stats.movedToDlq += 1;
        break;
    }
    RqueueMessage rqueueMessage = event.getJob().getRqueueMessage();
    MessageMetadata messageMetadata = event.getJob().getMessageMetadata();
    if (rqueueMessage.getFailureCount() != 0) {
      stats.retried += 1;
    }
    stats.minExecution = Math.min(stats.minExecution, messageMetadata.getTotalExecutionTime());
    stats.maxExecution = Math.max(stats.maxExecution, messageMetadata.getTotalExecutionTime());
    stats.jobCount += 1;
    stats.totalExecutionTime += messageMetadata.getTotalExecutionTime();
  }

  @Test
  void onApplicationEvent() throws TimedOutException {
    if (LocalDateTime.now(ZoneOffset.UTC).getHour() == 23) {
      log.info("This test cannot be run at this time");
      return;
    }
    String id = "__rq::q-stat::" + queueName;
    doReturn(500).when(rqueueWebConfig).getAggregateEventLockDurationInMs();
    doReturn(id).when(rqueueConfig).getQueueStatisticsKey(queueName);
    doReturn("__rq::lock::" + id).when(rqueueConfig).getLockKey(id);

    doReturn(true)
        .when(rqueueLockManager)
        .acquireLock(eq("__rq::lock::" + id), anyString(), eq(Duration.ofMillis(500L)));
    List<QueueStatistics> queueStatistics = new ArrayList<>();
    doAnswer(
        invocation -> {
          queueStatistics.add(invocation.getArgument(0));
          return null;
        })
        .when(rqueueQStatsDao)
        .save(any());

    RqueueExecutionEvent event;
    TasksStat tasksStat = new TasksStat();
    int totalEvents = 0;
    for (; totalEvents < 498; totalEvents++) {
      event = generateTaskEvent();
      addEvent(event, tasksStat, true);
    }
    if (tasksStat.movedToDlq == 0) {
      totalEvents += 1;
      event = generateTaskEventWithStatus(MessageStatus.MOVED_TO_DLQ);
      addEvent(event, tasksStat, true);
    }
    if (tasksStat.success == 0) {
      totalEvents += 1;
      event = generateTaskEventWithStatus(MessageStatus.SUCCESSFUL);
      addEvent(event, tasksStat, true);
    }
    if (tasksStat.discarded == 0) {
      totalEvents += 1;
      event = generateTaskEventWithStatus(MessageStatus.DISCARDED);
      addEvent(event, tasksStat, totalEvents < 500);
    }
    if (tasksStat.retried == 0) {
      totalEvents += 1;
      event = generateTaskEventWithStatus(MessageStatus.DISCARDED);
      event.getJob().getRqueueMessage().setFailureCount(10);
      addEvent(event, tasksStat, totalEvents < 500);
    }
    for (; totalEvents < 501; totalEvents++) {
      event = generateTaskEvent();
      addEvent(event, tasksStat, totalEvents < 500);
    }

    TimeoutUtils.waitFor(
        () -> !queueStatistics.isEmpty(), 60 * Constants.ONE_MILLI, "stats to be saved.");
    QueueStatistics statistics = queueStatistics.get(0);
    String date = DateTimeUtils.today().toString();
    assertEquals(statistics.getId(), id);
    QueueStatisticsTest.validate(statistics, 1);
    QueueStatisticsTest.checkNonNull(statistics, date);
    assertEquals(tasksStat.jobRunTime(), statistics.jobRunTime(date));
    assertEquals(tasksStat.discarded, statistics.tasksDiscarded(date));
    assertEquals(tasksStat.success, statistics.tasksSuccessful(date));
    assertEquals(tasksStat.movedToDlq, statistics.tasksMovedToDeadLetter(date));
    assertEquals(tasksStat.retried, statistics.tasksRetried(date));
  }

  @AfterEach
  public void clean() throws Exception {
    rqueueJobMetricsAggregatorService.destroy();
  }
}
