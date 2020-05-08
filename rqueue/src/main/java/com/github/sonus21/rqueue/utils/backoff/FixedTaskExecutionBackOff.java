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

package com.github.sonus21.rqueue.utils.backoff;

import com.github.sonus21.rqueue.core.RqueueMessage;

public class FixedTaskExecutionBackOff implements TaskExecutionBackOff {
  /** The default task delay: 5000 ms = 5 seconds. */
  public static final long DEFAULT_INTERVAL = 5000;

  private long interval = DEFAULT_INTERVAL;
  private int maxRetries = Integer.MAX_VALUE;

  /**
   * Create an instance with an interval of {@value #DEFAULT_INTERVAL} ms and maximum value of
   * retries.
   */
  public FixedTaskExecutionBackOff() {}

  /**
   * Create an instance.
   *
   * @param interval the delay between two attempts.
   * @param maxRetries the maximum number of retries.
   */
  public FixedTaskExecutionBackOff(long interval, int maxRetries) {
    this.interval = interval;
    this.maxRetries = maxRetries;
  }

  /** Set the delay between two attempts in milliseconds. */
  public void setInterval(long interval) {
    this.interval = interval;
  }

  /** Return the delay between two attempts in milliseconds. */
  public long getInterval() {
    return this.interval;
  }

  /** Set the maximum number of retries */
  public void setMaxRetries(int maxRetries) {
    this.maxRetries = maxRetries;
  }

  /** Return the maximum number of retries. */
  public int getMaxRetries() {
    return this.maxRetries;
  }

  @Override
  public long nextBackOff(Object object, RqueueMessage rqueueMessage, int failureCount) {
    if (failureCount > getMaxRetries()) {
      return STOP;
    }
    return getInterval();
  }
}
