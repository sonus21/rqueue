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

package com.github.sonus21.rqueue.web.service;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;

import com.github.sonus21.rqueue.common.RqueueLockManager;
import com.github.sonus21.rqueue.config.RqueueConfig;
import com.github.sonus21.rqueue.config.RqueueWebConfig;
import com.github.sonus21.rqueue.core.RqueueMessage;
import com.github.sonus21.rqueue.exception.TimedOutException;
import com.github.sonus21.rqueue.models.aggregator.TasksStat;
import com.github.sonus21.rqueue.models.db.MessageMetadata;
import com.github.sonus21.rqueue.models.db.MessageStatus;
import com.github.sonus21.rqueue.models.db.QueueStatistics;
import com.github.sonus21.rqueue.models.db.QueueStatisticsTest;
import com.github.sonus21.rqueue.models.enums.TaskStatus;
import com.github.sonus21.rqueue.models.event.RqueueExecutionEvent;
import com.github.sonus21.rqueue.utils.Constants;
import com.github.sonus21.rqueue.utils.DateTimeUtils;
import com.github.sonus21.rqueue.utils.TestUtils;
import com.github.sonus21.rqueue.utils.TimeoutUtils;
import com.github.sonus21.rqueue.web.dao.RqueueQStatsDao;
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

@Slf4j
public class RqueueTaskAggregatorServiceTest {
  private RqueueQStatsDao rqueueQStatsDao = mock(RqueueQStatsDao.class);
  private RqueueWebConfig rqueueWebConfig = mock(RqueueWebConfig.class);
  private RqueueLockManager rqueueLockManager = mock(RqueueLockManager.class);
  private RqueueConfig rqueueConfig = mock(RqueueConfig.class);
  private RqueueTaskAggregatorService rqueueTaskAggregatorService =
      new RqueueTaskAggregatorService(
          rqueueConfig, rqueueWebConfig, rqueueLockManager, rqueueQStatsDao);
  private String queueName = "test-queue";

  @BeforeEach
  public void initService() throws IllegalAccessException {
    doReturn(true).when(rqueueWebConfig).isCollectListenerStats();
    doReturn(1).when(rqueueWebConfig).getStatsAggregatorThreadCount();
    doReturn(100).when(rqueueWebConfig).getAggregateEventWaitTime();
    doReturn(100).when(rqueueWebConfig).getAggregateShutdownWaitTime();
    doReturn(180).when(rqueueWebConfig).getHistoryDay();
    doReturn(500).when(rqueueWebConfig).getAggregateEventCount();
    this.rqueueTaskAggregatorService.start();
    assertNotNull(
        FieldUtils.readField(this.rqueueTaskAggregatorService, "queueNameToEvents", true));
    assertNotNull(FieldUtils.readField(this.rqueueTaskAggregatorService, "queue", true));
    assertNotNull(FieldUtils.readField(this.rqueueTaskAggregatorService, "taskExecutor", true));
  }

  private RqueueExecutionEvent generateTaskEventWithStatus(TaskStatus status) {
    double r = Math.random();
    RqueueMessage rqueueMessage = new RqueueMessage("test-queue", "test", null, null);
    MessageMetadata messageMetadata =
        new MessageMetadata(rqueueMessage.getId(), MessageStatus.FAILED);
    messageMetadata.setTotalExecutionTime(10 + (long) r * 10000);
    rqueueMessage.setFailureCount((int) r * 10);
    return new RqueueExecutionEvent(
        TestUtils.createQueueDetail(queueName), rqueueMessage, status, messageMetadata);
  }

  private RqueueExecutionEvent generateTaskEvent() {
    double r = Math.random();
    TaskStatus taskStatus;
    if (r < 0.3) {
      taskStatus = TaskStatus.SUCCESSFUL;
    } else if (r < 0.6) {
      taskStatus = TaskStatus.DISCARDED;
    } else {
      taskStatus = TaskStatus.MOVED_TO_DLQ;
    }
    return generateTaskEventWithStatus(taskStatus);
  }

  private void addEvent(RqueueExecutionEvent event, TasksStat stats, boolean updateTaskStat) {
    rqueueTaskAggregatorService.onApplicationEvent(event);
    if (!updateTaskStat) {
      return;
    }
    switch (event.getStatus()) {
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
    RqueueMessage rqueueMessage = event.getRqueueMessage();
    MessageMetadata messageMetadata = event.getMessageMetadata();
    if (rqueueMessage.getFailureCount() != 0) {
      stats.retried += 1;
    }
    stats.minExecution = Math.min(stats.minExecution, messageMetadata.getTotalExecutionTime());
    stats.maxExecution = Math.max(stats.maxExecution, messageMetadata.getTotalExecutionTime());
    stats.jobCount += 1;
    stats.totalExecutionTime += messageMetadata.getTotalExecutionTime();
  }

  @Test
  public void onApplicationEvent() throws TimedOutException {
    if (LocalDateTime.now(ZoneOffset.UTC).getHour() == 23) {
      log.info("This test cannot be run at this time");
      return;
    }
    String id = "__rq::q-stat::" + queueName;
    doReturn(id).when(rqueueConfig).getQueueStatisticsKey(queueName);
    doReturn("__rq::lock::" + id).when(rqueueConfig).getLockKey(id);

    doReturn(true)
        .when(rqueueLockManager)
        .acquireLock(
            "__rq::lock::" + id,
            Duration.ofSeconds(Constants.AGGREGATION_LOCK_DURATION_IN_SECONDS));
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
      event = generateTaskEventWithStatus(TaskStatus.MOVED_TO_DLQ);
      addEvent(event, tasksStat, true);
    }
    if (tasksStat.success == 0) {
      totalEvents += 1;
      event = generateTaskEventWithStatus(TaskStatus.SUCCESSFUL);
      addEvent(event, tasksStat, true);
    }
    if (tasksStat.discarded == 0) {
      totalEvents += 1;
      event = generateTaskEventWithStatus(TaskStatus.DISCARDED);
      addEvent(event, tasksStat, totalEvents < 500);
    }
    if (tasksStat.retried == 0) {
      totalEvents += 1;
      event = generateTaskEventWithStatus(TaskStatus.DISCARDED);
      event.getRqueueMessage().setFailureCount(10);
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
    rqueueTaskAggregatorService.destroy();
  }
}
