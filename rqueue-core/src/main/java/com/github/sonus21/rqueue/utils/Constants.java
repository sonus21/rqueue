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

package com.github.sonus21.rqueue.utils;

import com.github.sonus21.rqueue.config.RqueueConfig;

public final class Constants {

  public static final String BLANK = "";
  public static final long ONE_MILLI = 1000;
  public static final int ONE_MILLI_INT = 1000;
  public static final int SECONDS_IN_A_MINUTE = 60;
  public static final long MILLIS_IN_A_MINUTE = SECONDS_IN_A_MINUTE * ONE_MILLI;
  public static final int MINUTES_IN_AN_HOUR = 60;
  public static final int HOURS_IN_A_DAY = 24;
  public static final int DAYS_IN_A_MONTH = 30;
  public static final int DAYS_IN_A_WEEK = 7;
  public static final int MINUTES_IN_A_DAY = HOURS_IN_A_DAY * MINUTES_IN_AN_HOUR;
  public static final int SECONDS_IN_A_DAY = MINUTES_IN_A_DAY * SECONDS_IN_A_MINUTE;
  public static final long MILLIS_IN_A_DAY = SECONDS_IN_A_DAY * ONE_MILLI;
  public static final long DEFAULT_SCRIPT_EXECUTION_TIME = 5 * ONE_MILLI;
  public static final long MIN_DELAY = 100L;
  public static final long MIN_SCHEDULE_INTERVAL = 100L;
  public static final long MIN_EXECUTION_TIME = MIN_DELAY;
  public static final long DELTA_BETWEEN_RE_ENQUEUE_TIME = ONE_MILLI;
  public static final int DEFAULT_RETRY_DEAD_LETTER_QUEUE = 3;
  public static final int MAX_MESSAGES = 100;
  public static final int DEFAULT_WORKER_COUNT_PER_QUEUE = 2;
  public static final String GITHUB_API_FOR_LATEST_RELEASE =
      "https://api.github.com/repos/sonus21/rqueue/releases/latest";
  public static final String DEFAULT_PRIORITY_KEY = "DEFAULT_PRIORITY";
  public static final String DEFAULT_PRIORITY_GROUP = "\uD83D\uDCAF";
  public static final String REDIS_KEY_SEPARATOR = "::";
  public static final int MAX_STACKTRACE_LENGTH = 3000;
  public static final String Comma = ",";
  public static final String FORWARD_SLASH = "/";
  public static final int MIN_BATCH_SIZE = 1;
  public static final long MIN_VISIBILITY = 10;
  public static final int BATCH_SIZE_FOR_CONCURRENCY_BASED_LISTENER = 10;
  public static final int MIN_CONCURRENCY = 1;
  public static final long MINIMUM_JOB_PERIOD = 1000L;
  public static final String QUEUE_CRUD_LOCK_KEY_PREFIX = "q-crud::";

  private Constants() {
  }

  public static String getQueueCrudLockKey(RqueueConfig rqueueConfig, String queueName) {
    return rqueueConfig.getLockKey(QUEUE_CRUD_LOCK_KEY_PREFIX + queueName);
  }
}
