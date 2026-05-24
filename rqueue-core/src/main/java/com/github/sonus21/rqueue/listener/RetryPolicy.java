/*
 * Copyright (c) 2026 Sonu Kumar
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

package com.github.sonus21.rqueue.listener;

import com.github.sonus21.rqueue.config.RqueueConfig;
import com.github.sonus21.rqueue.core.RqueueMessage;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;

@NoArgsConstructor(access = AccessLevel.PRIVATE)
final class RetryPolicy {

  static final int UNLIMITED_RETRY_LIMIT = 100_000;

  static int maxRetryCount(RqueueMessage rqueueMessage, QueueDetail queueDetail) {
    int maxRetryCount = rqueueMessage.getRetryCount() == null
        ? queueDetail.getNumRetry()
        : rqueueMessage.getRetryCount();
    if (maxRetryCount == Integer.MAX_VALUE) {
      return UNLIMITED_RETRY_LIMIT;
    }
    return maxRetryCount;
  }

  static int remainingRetryCount(
      RqueueMessage rqueueMessage, QueueDetail queueDetail, int failureCount) {
    int maxRetryCount = maxRetryCount(rqueueMessage, queueDetail);
    return Math.max(0, maxRetryCount - failureCount);
  }

  static int retryCountForPoll(
      RqueueConfig rqueueConfig,
      RqueueMessage rqueueMessage,
      QueueDetail queueDetail,
      int failureCount) {
    int remainingRetryCount = remainingRetryCount(rqueueMessage, queueDetail, failureCount);
    if (rqueueConfig.getRetryPerPoll() == -1) {
      return remainingRetryCount;
    }
    return Math.min(rqueueConfig.getRetryPerPoll(), remainingRetryCount);
  }

  static boolean isExhausted(
      RqueueMessage rqueueMessage, QueueDetail queueDetail, int failureCount) {
    return remainingRetryCount(rqueueMessage, queueDetail, failureCount) == 0;
  }
}
