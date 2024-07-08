/*
 * Copyright (c) 2020-2024 Sonu Kumar
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

package com.github.sonus21.rqueue.utils.backoff;

import com.github.sonus21.rqueue.core.RqueueMessage;

public interface TaskExecutionBackOff {

  /**
   * Return value of {@link #nextBackOff(Object, RqueueMessage, int)} that indicates that the task
   * should not be retried further.
   */
  long STOP = -1;
  /**
   * Return this value, so that it will not retry post-failure, and it won't move to DLQ as well.
   */
  long DO_NOT_RETRY = -2;

  /**
   * Return the number of milliseconds to wait for the same message to be consumed.
   * <p>Return {@value #STOP} to indicate that no further enqueue should be made for the message.
   * </p>
   *
   * @param message       message that's fetched
   * @param rqueueMessage raw message
   * @param failureCount  number of times this message has failed.
   * @return backoff in the millisecond.
   */
  long nextBackOff(Object message, RqueueMessage rqueueMessage, int failureCount);

  /**
   * Return the number of milliseconds to wait for the same message to be consumed.
   * <p>Return {@value #DO_NOT_RETRY} to indicate that no further retry should be made</p>
   * <p>Return {@value #STOP} to indicate message should be moved to DLQ if DLQ is set</p>
   *
   * @param message       message that's fetched
   * @param rqueueMessage raw message
   * @param failureCount  number of times this message has failed.
   * @param throwable     the exception that has occurred
   * @return backoff in the millisecond.
   */
  default long nextBackOff(Object message, RqueueMessage rqueueMessage, int failureCount,
      Throwable throwable) {
    return nextBackOff(message, rqueueMessage, failureCount);
  }
}
