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

package com.github.sonus21.rqueue.listener;

import com.github.sonus21.rqueue.utils.ThreadUtils.QueueThread;
import com.github.sonus21.rqueue.utils.TimeoutUtils;
import java.util.Collections;
import org.slf4j.event.Level;

class DefaultRqueuePoller extends RqueueMessagePoller {
  private final QueueDetail queueDetail;
  private final QueueThread queueThread;

  DefaultRqueuePoller(
      QueueThread queueThread,
      QueueDetail queueDetail,
      RqueueMessageListenerContainer container,
      PostProcessingHandler postProcessingHandler,
      int retryPerPoll) {
    super(queueDetail.getName(), container, postProcessingHandler, retryPerPoll);
    this.queueDetail = queueDetail;
    this.queueThread = queueThread;
    this.queues = Collections.singletonList(queueDetail.getName());
  }

  @Override
  long getSemaphoreWaiTime() {
    return getPollingInterval();
  }

  @Override
  void deactivate(int index, String queue, DeactivateType deactivateType) {
    if (deactivateType == DeactivateType.SEMAPHORE_UNAVAILABLE
        || deactivateType == DeactivateType.NO_MESSAGE) {
      TimeoutUtils.sleepLog(getPollingInterval(), false);
    } else if (deactivateType == DeactivateType.POLL_FAILED) {
      TimeoutUtils.sleepLog(getBackOffTime(), false);
    }
  }

  @Override
  void start() {
    log(Level.DEBUG, "Running Queue {}", null, queueDetail.getName());
    while (!shouldExit()) {
      poll(-1, queueDetail.getName(), queueDetail, queueThread);
    }
  }
}
