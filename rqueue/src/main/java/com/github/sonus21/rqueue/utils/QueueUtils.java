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

import java.util.Collections;
import java.util.Map;

public class QueueUtils {
  private static final String QUEUE_NAME = "QUEUE_NAME";
  private static final String LOCK_KEY_PREFIX = "__rq::lock::";
  private static final String DELAYED_QUEUE_PREFIX = "__rq::d-queue::";
  private static final String DELAYED_QUEUE_CHANNEL_PREFIX = "__rq::d-channel::";
  private static final String PROCESSING_QUEUE_PREFIX = "__rq::p-queue::";
  private static final String PROCESSING_QUEUE_CHANNEL_PREFIX = "__rq::p-channel::";
  private static final String QUEUES_KEY = "__rq::queues";
  private static final String QUEUE_STATISTIC_PREFIX = "__rq::q-stat::";
  private static final String QUEUE_CONFIG_PREFIX = "__rq::q-config::";
  private static final String MESSAGE_META_DATA_KEY_PREFIX = "__rq::m-mdata::";

  private QueueUtils() {}

  public static String getMessageHeaderKey() {
    return QUEUE_NAME;
  }

  public static Map<String, Object> getQueueHeaders(String queueName) {
    return Collections.singletonMap(getMessageHeaderKey(), queueName);
  }

  public static String getDelayedQueueName(String queueName) {
    return DELAYED_QUEUE_PREFIX + queueName;
  }

  public static String getDelayedQueueChannelName(String queueName) {
    return DELAYED_QUEUE_CHANNEL_PREFIX + queueName;
  }

  public static String getProcessingQueueName(String queueName) {
    return PROCESSING_QUEUE_PREFIX + queueName;
  }

  public static String getProcessingQueueChannelName(String queueName) {
    return PROCESSING_QUEUE_CHANNEL_PREFIX + queueName;
  }

  public static long getMessageReEnqueueTimeWithDelay(long currentTime, long maxDelay) {
    return currentTime + maxDelay;
  }

  public static String getQueuesKey() {
    return QUEUES_KEY;
  }

  public static String getQueueStatKey(String queueName) {
    return QUEUE_STATISTIC_PREFIX + queueName;
  }

  public static String getQueueConfigKey(String queueName) {
    return QUEUE_CONFIG_PREFIX + queueName;
  }

  public static String getLockKey(Object id) {
    return LOCK_KEY_PREFIX + id;
  }

  public static boolean isTimeQueue(String name) {
    return name.startsWith(DELAYED_QUEUE_PREFIX);
  }

  public static String getMessageMetadataKey(String id) {
    return MESSAGE_META_DATA_KEY_PREFIX + id;
  }
}
