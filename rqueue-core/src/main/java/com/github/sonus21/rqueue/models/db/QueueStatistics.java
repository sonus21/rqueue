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

import com.github.sonus21.rqueue.models.SerializableBase;
import com.github.sonus21.rqueue.models.aggregator.TasksStat;
import java.time.LocalDate;
import java.util.HashMap;
import java.util.Map;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.ToString;

@Getter
@Setter
@NoArgsConstructor
@EqualsAndHashCode(callSuper = false, onlyExplicitlyIncluded = true)
@ToString
public class QueueStatistics extends SerializableBase {

  private static final long serialVersionUID = -4931206278693499365L;
  @EqualsAndHashCode.Include
  private String id;
  private Long startEpochDate;
  private Map<String, Long> tasksMovedToDeadLetter;
  private Map<String, Long> tasksDiscarded;
  private Map<String, Long> tasksSuccessful;
  private Map<String, Long> tasksRetried;
  private Map<String, JobRunTime> jobRunTime;
  private Long createdOn;
  private Long updatedOn;

  public QueueStatistics(String id) {
    this.id = id;
    this.createdOn = System.currentTimeMillis();
    this.updatedOn = createdOn;
    this.tasksMovedToDeadLetter = new HashMap<>();
    this.tasksDiscarded = new HashMap<>();
    this.tasksSuccessful = new HashMap<>();
    this.tasksRetried = new HashMap<>();
    this.jobRunTime = new HashMap<>();
  }

  public void updateTime() {
    this.updatedOn = System.currentTimeMillis();
  }

  public void incrementDeadLetter(String date, long delta) {
    long val = tasksMovedToDeadLetter.getOrDefault(date, 0L);
    tasksMovedToDeadLetter.put(date, val + delta);
  }

  public void update(TasksStat stat, String eventDate) {
    if (stat.movedToDlq > 0) {
      incrementDeadLetter(eventDate, stat.movedToDlq);
    }
    if (stat.discarded > 0) {
      incrementDiscard(eventDate, stat.discarded);
    }
    if (stat.success > 0) {
      incrementSuccessful(eventDate, stat.success);
    }
    if (stat.retried > 0) {
      incrementRetry(eventDate, stat.retried);
    }
    updateJobExecutionTime(eventDate, stat.jobRunTime());
  }

  public void incrementDiscard(String date, long delta) {
    long val = tasksDiscarded.getOrDefault(date, 0L);
    tasksDiscarded.put(date, val + delta);
  }

  public void incrementSuccessful(String date, long delta) {
    long val = tasksSuccessful.getOrDefault(date, 0L);
    tasksSuccessful.put(date, val + delta);
  }

  public void updateJobExecutionTime(String date, JobRunTime jobRunTimeDelta) {
    JobRunTime val = this.jobRunTime.getOrDefault(date, null);
    if (val == null) {
      this.jobRunTime.put(date, jobRunTimeDelta);
    } else {
      val.setMax(Math.max(val.getMax(), jobRunTimeDelta.getMax()));
      val.setMin(Math.min(val.getMin(), jobRunTimeDelta.getMin()));
      val.setJobCount(val.getJobCount() + jobRunTimeDelta.getJobCount());
      val.setTotalExecutionTime(
          val.getTotalExecutionTime() + jobRunTimeDelta.getTotalExecutionTime());
      this.jobRunTime.put(date, val);
    }
  }

  private void cleanData(String date) {
    tasksMovedToDeadLetter.remove(date);
    tasksDiscarded.remove(date);
    tasksSuccessful.remove(date);
    tasksRetried.remove(date);
    jobRunTime.remove(date);
  }

  public void pruneStats(LocalDate date, int maxDays) {
    if (startEpochDate == null) {
      startEpochDate = date.toEpochDay();
      return;
    }
    long daysDifference = date.toEpochDay() - startEpochDate;
    long extraDaysData = daysDifference - maxDays;
    if (extraDaysData > 0) {
      LocalDate startDate = date.minusDays(maxDays);
      this.startEpochDate = startDate.toEpochDay();
      for (int i = 0; i <= extraDaysData; i++) {
        cleanData(startDate.minusDays(i).toString());
      }
    }
  }

  public void incrementRetry(String date, long delta) {
    long val = tasksRetried.getOrDefault(date, 0L);
    tasksRetried.put(date, val + delta);
  }

  public long tasksDiscarded(String date) {
    return tasksDiscarded.getOrDefault(date, 0L);
  }

  public long tasksSuccessful(String date) {
    return tasksSuccessful.getOrDefault(date, 0L);
  }

  public long tasksMovedToDeadLetter(String date) {
    return tasksMovedToDeadLetter.getOrDefault(date, 0L);
  }

  public long tasksRetried(String date) {
    return tasksRetried.getOrDefault(date, 0L);
  }

  public JobRunTime jobRunTime(String date) {
    return jobRunTime.getOrDefault(date, new JobRunTime());
  }
}
