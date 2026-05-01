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

package com.github.sonus21.rqueue.models.db;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.LocalDate;

/**
 * Shared fixture helpers for {@link QueueStatistics}. Lives in {@code rqueue-test-util/src/main}
 * so it can be consumed by both {@code rqueue-core}'s tests and downstream backend modules
 * ({@code rqueue-redis}, {@code rqueue-nats}) that exercise dashboard/chart code.
 */
public final class QueueStatisticsFixtures {

  private QueueStatisticsFixtures() {}

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
}
