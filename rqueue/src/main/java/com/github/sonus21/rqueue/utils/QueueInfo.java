/*
 * Copyright 2019 Sonu Kumar
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

public abstract class QueueInfo {
  public static final String QUEUE_NAME = "QUEUE_NAME";
  private static final String DELAYED_QUEUE_PREFIX = "rqueue-delay::";
  private static final String CHANNEL_PREFIX = "rqueue-channel::";
  private static final String PROCESSING_PREFIX = "rqueue-processing::";
  private static final String PROCESSING_CHANNEL_PREFIX = "rqueue-processing-channel::";
  // 15 minutes in millis
  private static final long MAX_MESSAGE_PROCESSING_TIME = 15 * 60 * 1000L;

  public static Map<String, Object> getQueueHeaders(String queueName) {
    return Collections.singletonMap(QUEUE_NAME, queueName);
  }

  public static String getTimeQueueName(String queueName) {
    return DELAYED_QUEUE_PREFIX + queueName;
  }

  public static String getChannelName(String queueName) {
    return CHANNEL_PREFIX + queueName;
  }

  public static String getProcessingQueueName(String queueName) {
    return PROCESSING_PREFIX + queueName;
  }

  public static String getProcessingQueueChannelName(String queueName) {
    return PROCESSING_CHANNEL_PREFIX + queueName;
  }

  public static long getMessageReEnqueueTime() {
    return getMessageReEnqueueTime(System.currentTimeMillis());
  }

  public static long getMessageReEnqueueTime(long currentTime) {
    return currentTime + MAX_MESSAGE_PROCESSING_TIME;
  }
}
