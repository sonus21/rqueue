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

package com.github.sonus21.rqueue.models.db;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.github.sonus21.TestBase;
import com.github.sonus21.rqueue.CoreUnitTest;
import java.time.LocalDate;
import org.junit.jupiter.api.Test;

@CoreUnitTest
public class QueueStatisticsTest extends TestBase {

  private final String id = "__rq::q-stat::slow-queue";

  public static void validate(QueueStatistics queueStatistics, int count) {
    assertEquals(count, queueStatistics.getJobRunTime().size());
    assertEquals(count, queueStatistics.getTasksSuccessful().size());
    assertEquals(count, queueStatistics.getTasksDiscarded().size());
    assertEquals(count, queueStatistics.getTasksMovedToDeadLetter().size());
    assertEquals(count, queueStatistics.getTasksRetried().size());
  }

  public static void checkNonNull(QueueStatistics queueStatistics, String date) {
    assertNotNull(queueStatistics.jobRunTime(date));
    assertTrue(queueStatistics.tasksSuccessful(date) > 0);
    assertTrue(queueStatistics.tasksDiscarded(date) > 0);
    assertTrue(queueStatistics.tasksMovedToDeadLetter(date) > 0);
    assertTrue(queueStatistics.tasksRetried(date) > 0);
  }

  public static void addData(QueueStatistics queueStatistics, LocalDate localDate, int day) {
    String date = localDate.minusDays(day).toString();
    int val = 1 + (int) (Math.random() * 100);
    queueStatistics.incrementSuccessful(date, val);
    queueStatistics.incrementDiscard(date, val);
    queueStatistics.incrementDeadLetter(date, val);
    queueStatistics.incrementRetry(date, val);
    int val2 = 1 + (int) (Math.random() * 100);
    JobRunTime jobRunTime = new JobRunTime(val, val2, val * val2, val2);
    queueStatistics.updateJobExecutionTime(date, jobRunTime);
  }

  @Test
  void incrementDeadLetter() {
    QueueStatistics queueStatistics = new QueueStatistics(id);
    assertNotNull(queueStatistics.getTasksMovedToDeadLetter());

    queueStatistics.incrementDeadLetter("2020-03-11", 10L);
    assertEquals(10, queueStatistics.getTasksMovedToDeadLetter().get("2020-03-11"));

    queueStatistics.incrementDeadLetter("2020-03-12", 100L);
    assertEquals(2, queueStatistics.getTasksMovedToDeadLetter().size());
    assertEquals(100, queueStatistics.getTasksMovedToDeadLetter().get("2020-03-12"));

    queueStatistics.incrementDeadLetter("2020-03-12", 100L);
    assertEquals(2, queueStatistics.getTasksMovedToDeadLetter().size());
    assertEquals(200, queueStatistics.getTasksMovedToDeadLetter().get("2020-03-12"));
  }

  @Test
  void incrementDiscard() {
    QueueStatistics queueStatistics = new QueueStatistics(id);
    assertNotNull(queueStatistics.getTasksDiscarded());

    queueStatistics.incrementDiscard("2020-03-11", 10L);
    assertEquals(10L, queueStatistics.tasksDiscarded("2020-03-11"));

    queueStatistics.incrementDiscard("2020-03-12", 100L);
    assertEquals(2, queueStatistics.getTasksDiscarded().size());
    assertEquals(100L, queueStatistics.getTasksDiscarded().get("2020-03-12"));

    queueStatistics.incrementDiscard("2020-03-12", 100L);
    assertEquals(2, queueStatistics.getTasksDiscarded().size());
    assertEquals(200L, queueStatistics.getTasksDiscarded().get("2020-03-12"));
  }

  @Test
  void incrementRetry() {
    QueueStatistics queueStatistics = new QueueStatistics(id);
    assertNotNull(queueStatistics.getTasksRetried());

    queueStatistics.incrementRetry("2020-03-11", 10L);
    assertEquals(10L, queueStatistics.tasksRetried("2020-03-11"));

    queueStatistics.incrementRetry("2020-03-12", 100L);
    assertEquals(2, queueStatistics.getTasksRetried().size());
    assertEquals(100L, queueStatistics.getTasksRetried().get("2020-03-12"));

    queueStatistics.incrementRetry("2020-03-12", 100L);
    assertEquals(2, queueStatistics.getTasksRetried().size());
    assertEquals(200L, queueStatistics.getTasksRetried().get("2020-03-12"));
  }

  @Test
  void incrementSuccessful() {
    QueueStatistics queueStatistics = new QueueStatistics(id);
    assertNotNull(queueStatistics.getTasksSuccessful());

    queueStatistics.incrementSuccessful("2020-03-11", 10L);
    assertEquals(10L, queueStatistics.tasksSuccessful("2020-03-11"));

    queueStatistics.incrementSuccessful("2020-03-12", 100L);
    assertEquals(2, queueStatistics.getTasksSuccessful().size());
    assertEquals(100L, queueStatistics.getTasksSuccessful().get("2020-03-12"));

    queueStatistics.incrementSuccessful("2020-03-12", 100L);
    assertEquals(2, queueStatistics.getTasksSuccessful().size());
    assertEquals(200L, queueStatistics.getTasksSuccessful().get("2020-03-12"));
  }

  @Test
  void updateJobExecutionTime() {
    JobRunTime jobRunTime = new JobRunTime(100L, 300L, 1100, 10);
    JobRunTime jobRunTime2 = new JobRunTime(50L, 250L, 100, 100);
    JobRunTime jobRunTime3 = new JobRunTime(100L, 400L, 1000, 4);

    QueueStatistics queueStatistics = new QueueStatistics(id);
    assertNotNull(queueStatistics.getJobRunTime());

    queueStatistics.updateJobExecutionTime("2020-03-11", jobRunTime);
    assertEquals(jobRunTime, queueStatistics.getJobRunTime().get("2020-03-11"));

    queueStatistics.updateJobExecutionTime("2020-03-12", jobRunTime);
    assertEquals(2, queueStatistics.getJobRunTime().size());
    assertEquals(jobRunTime, queueStatistics.getJobRunTime().get("2020-03-12"));

    queueStatistics.updateJobExecutionTime("2020-03-12", jobRunTime);
    assertEquals(2, queueStatistics.getJobRunTime().size());
    assertEquals(
        new JobRunTime(100, 300, 2200, 20), queueStatistics.getJobRunTime().get("2020-03-12"));

    queueStatistics.updateJobExecutionTime("2020-03-12", jobRunTime2);
    assertEquals(2, queueStatistics.getJobRunTime().size());
    assertEquals(
        new JobRunTime(50, 300, 2300, 120), queueStatistics.getJobRunTime().get("2020-03-12"));

    queueStatistics.updateJobExecutionTime("2020-03-12", jobRunTime3);
    assertEquals(
        new JobRunTime(50, 400, 3300, 124), queueStatistics.getJobRunTime().get("2020-03-12"));
  }

  @Test
  void pruneStats() {
    QueueStatistics queueStatistics = new QueueStatistics(id);
    LocalDate localDate = LocalDate.parse("2020-04-11");
    queueStatistics.pruneStats(localDate, 7);
    assertNotNull(queueStatistics.getStartEpochDate());
    for (int i = 0; i < 7; i++) {
      addData(queueStatistics, localDate, i);
    }
    validate(queueStatistics, 7);
    queueStatistics.pruneStats(localDate, 7);
    validate(queueStatistics, 7);

    for (int i = 7; i < 14; i++) {
      addData(queueStatistics, localDate, i);
    }
    queueStatistics.setStartEpochDate(queueStatistics.getStartEpochDate() - 14);
    validate(queueStatistics, 14);
    queueStatistics.pruneStats(localDate, 7);
    validate(queueStatistics, 7);
    for (int i = 0; i < 7; i++) {
      String date = localDate.minusDays(i).toString();
      checkNonNull(queueStatistics, date);
    }
  }

  @Test
  void construct() {
    QueueStatistics queueStatistics = new QueueStatistics(id);
    assertEquals(id, queueStatistics.getId());
    assertNotNull(queueStatistics.getCreatedOn());
    assertEquals(queueStatistics.getCreatedOn(), queueStatistics.getUpdatedOn());
    assertNotNull(queueStatistics.getTasksRetried());
    assertNotNull(queueStatistics.getTasksDiscarded());
    assertNotNull(queueStatistics.getJobRunTime());
    assertNotNull(queueStatistics.getTasksMovedToDeadLetter());
    assertNotNull(queueStatistics.getTasksRetried());
    assertNull(queueStatistics.getStartEpochDate());
  }
}
