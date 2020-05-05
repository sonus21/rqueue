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

public class SystemUtils {
  SystemUtils() {}

  private static final String LOCK_KEY_PREFIX = "__rq::lock::";
  private static final String QUEUES_KEY = "__rq::queues";
  private static final String QUEUE_STATISTIC_PREFIX = "__rq::q-stat::";
  private static final String QUEUE_CONFIG_PREFIX = "__rq::q-config::";
  private static final String VERSION_KEY = "__rq::version::";

  public static String getQueuesKey() {
    return QUEUES_KEY;
  }

  public static String getQueueStatKey(String queueName) {
    return QUEUE_STATISTIC_PREFIX + queueName;
  }

  public static String getQueueConfigKey(String queueName) {
    return QUEUE_CONFIG_PREFIX + queueName;
  }

  public static String getVersionKey() {
    return VERSION_KEY;
  }

  public static String getLockKey(Object id) {
    return LOCK_KEY_PREFIX + id;
  }
}
