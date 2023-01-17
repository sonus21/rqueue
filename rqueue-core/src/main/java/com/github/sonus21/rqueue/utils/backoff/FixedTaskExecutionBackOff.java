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

package com.github.sonus21.rqueue.utils.backoff;

import com.github.sonus21.rqueue.core.RqueueMessage;

/**
 * Implementation of the {@link TaskExecutionBackOff} class, that always return the same value,
 * except if number of failures have increased too high.
 *
 * <p>Default delay for the task is 5 seconds, and maximum number of retries is {@value
 * Integer#MAX_VALUE}.
 *
 * <p>There's another implementation of this, that provides exponential delay.
 *
 * @see ExponentialTaskExecutionBackOff
 */
public class FixedTaskExecutionBackOff implements TaskExecutionBackOff {

  /**
   * The default task delay: 5000 ms = 5 seconds.
   */
  public static final long DEFAULT_INTERVAL = 5000;

  private long interval = DEFAULT_INTERVAL;
  private int maxRetries = Integer.MAX_VALUE;

  /**
   * Create an instance with an interval of {@value #DEFAULT_INTERVAL} ms and maximum value of
   * retries.
   */
  public FixedTaskExecutionBackOff() {
  }

  /**
   * Create an instance.
   *
   * @param interval   the delay between two attempts.
   * @param maxRetries the maximum number of retries.
   */
  public FixedTaskExecutionBackOff(long interval, int maxRetries) {
    checkInterval(interval);
    checkMaxRetries(maxRetries);
    this.interval = interval;
    this.maxRetries = maxRetries;
  }

  /**
   * Return the delay between two attempts in milliseconds.
   *
   * @return the configured interval in milliseconds.
   */
  public long getInterval() {
    return this.interval;
  }

  /**
   * Set the delay between two attempts in milliseconds.
   *
   * @param interval delay between two attempts.
   */
  public void setInterval(long interval) {
    checkInterval(interval);
    this.interval = interval;
  }

  /**
   * Return the maximum number of retries.
   *
   * @return max retires
   */
  public int getMaxRetries() {
    return this.maxRetries;
  }

  /**
   * Set the maximum number of retries
   *
   * @param maxRetries max number of retires
   */
  public void setMaxRetries(int maxRetries) {
    checkMaxRetries(maxRetries);
    this.maxRetries = maxRetries;
  }

  @Override
  public long nextBackOff(Object message, RqueueMessage rqueueMessage, int failureCount) {
    if (failureCount >= getMaxRetries(message, rqueueMessage, failureCount)) {
      return STOP;
    }
    return getInterval(message, rqueueMessage, failureCount);
  }

  protected int getMaxRetries(Object message, RqueueMessage rqueueMessage, int failureCount) {
    return getMaxRetries();
  }

  protected long getInterval(Object message, RqueueMessage rqueueMessage, int failureCount) {
    return getInterval();
  }

  private void checkInterval(long interval) {
    if (interval <= 0) {
      throw new IllegalArgumentException("interval must be > 0");
    }
  }

  private void checkMaxRetries(int maxRetries) {
    if (maxRetries < 0) {
      throw new IllegalArgumentException("maxRetries must be > 0");
    }
  }

  @Override
  public boolean equals(Object other) {
    if (other instanceof FixedTaskExecutionBackOff) {
      return ((FixedTaskExecutionBackOff) other).getInterval() == getInterval()
          && ((FixedTaskExecutionBackOff) other).getMaxRetries() == getMaxRetries();
    }
    return false;
  }
}
