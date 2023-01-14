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

package com.github.sonus21.rqueue.listener;

import com.github.sonus21.rqueue.core.RqueueMessage;
import com.github.sonus21.rqueue.listener.RqueueMessageListenerContainer.QueueStateMgr;
import com.github.sonus21.rqueue.utils.QueueThreadPool;
import com.github.sonus21.rqueue.utils.RetryableRunnable;
import org.slf4j.Logger;

abstract class MessageContainerBase extends RetryableRunnable<Object> {

  protected final QueueStateMgr queueStateMgr;

  MessageContainerBase(Logger log, String groupName, QueueStateMgr queueStateMgr) {
    super(log, groupName);
    this.queueStateMgr = queueStateMgr;
  }

  boolean isQueueActive(String queueName) {
    return queueStateMgr.isQueueActive(queueName);
  }

  boolean eligibleForPolling(String queueName) {
    return isQueueNotPaused(queueName) && isQueueActive(queueName);
  }

  boolean isQueueNotPaused(String queueName) {
    return !isQueuePaused(queueName);
  }

  boolean isQueuePaused(String queueName) {
    return queueStateMgr.isQueuePaused(queueName);
  }

  protected void release(
      PostProcessingHandler postProcessingHandler,
      QueueThreadPool queueThreadPool,
      QueueDetail queueDetail,
      RqueueMessage message) {
    queueThreadPool.release();
    postProcessingHandler.parkMessageForRetry(message, message.getFailureCount(), -1, queueDetail);
  }
}
