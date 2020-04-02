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

package com.github.sonus21.rqueue.utils;

public class Constants {
  private Constants() {}

  public static final int SECONDS_IN_A_MINUTE = 60;
  public static final long ONE_MILLI = 1000;
  public static final long MAX_JOB_EXECUTION_TIME = 15 * SECONDS_IN_A_MINUTE * ONE_MILLI;
  public static final long DEFAULT_DELAY = 5 * ONE_MILLI;
  public static final long DEFAULT_SCRIPT_EXECUTION_TIME = DEFAULT_DELAY;
  public static final long DELTA_BETWEEN_RE_ENQUEUE_TIME = DEFAULT_DELAY;
  public static final long MIN_DELAY = 100L;
  public static final long TASK_ALIVE_TIME = -30 * Constants.ONE_MILLI;
  public static final int MAX_MESSAGES = 100;
  public static final int DEFAULT_WORKER_COUNT_PER_QUEUE = 2;
}
