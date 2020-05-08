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

public class ExponentialTaskExecutionBackOff implements TaskExecutionBackOff {
  /** The default initial delay for the any task: 2500 ms (2.5 seconds) */
  public static final long DEFAULT_INITIAL_INTERVAL = 2500L;

  /** The default multiplier (increases the interval by 50%). */
  public static final double DEFAULT_MULTIPLIER = 2;

  /** The default maximum delay for any task (1 hour) */
  public static final long DEFAULT_MAX_INTERVAL = 3600000L;

  private long initialInterval = DEFAULT_INITIAL_INTERVAL;
  private long maxInterval = DEFAULT_MAX_INTERVAL;
  private double multiplier = DEFAULT_MULTIPLIER;

  private int maxRetries = Integer.MAX_VALUE;

  /**
   * Create an instance with an initial interval of {@value #DEFAULT_INITIAL_INTERVAL} ms, maximum
   * interval of {@value #DEFAULT_MAX_INTERVAL}, multiplier of {@value #DEFAULT_MULTIPLIER} and
   * default unlimited retries.
   */
  public ExponentialTaskExecutionBackOff() {}

  /**
   * Create an instance.
   *
   * @param initialInterval the initial delay for the message
   * @param maxInterval the maximum delay for the message.
   * @param multiplier the factor at which delay would be increases.
   * @param maxRetries the maximum retry count for any message.
   */
  public ExponentialTaskExecutionBackOff(
      long initialInterval, long maxInterval, double multiplier, int maxRetries) {
    if (initialInterval <= 0) {
      throw new IllegalArgumentException("initialInterval must be > 0");
    }
    if (maxInterval < initialInterval) {
      throw new IllegalArgumentException(
          "maxInterval must be greater than or equal to initialInterval");
    }
    checkMultiplier(multiplier);
    this.initialInterval = initialInterval;
    this.maxInterval = maxInterval;
    this.multiplier = multiplier;
    this.maxRetries = maxRetries;
  }

  /** Set the initial interval in milliseconds. */
  public void setInitialInterval(long initialInterval) {
    this.initialInterval = initialInterval;
  }

  /** Return the initial interval in milliseconds. */
  public long getInitialInterval() {
    return this.initialInterval;
  }

  /** Set the maximum number of retries */
  public void setMaxRetries(int maxRetries) {
    this.maxRetries = maxRetries;
  }

  /** Return the maximum number of retries. */
  public int getMaxRetries() {
    return this.maxRetries;
  }

  /** get the multiplier */
  public double getMultiplier() {
    return multiplier;
  }

  /**
   * Set multiplier
   *
   * @param multiplier value of the multiplier
   */
  public void setMultiplier(double multiplier) {
    checkMultiplier(multiplier);
    this.multiplier = multiplier;
  }

  /**
   * get max interval
   *
   * @return long value
   */
  public long getMaxInterval() {
    return maxInterval;
  }

  /**
   * Set the max interval in milli seconds
   *
   * @param maxInterval value in milli seconds
   */
  public void setMaxInterval(long maxInterval) {
    this.maxInterval = maxInterval;
  }

  private void checkMultiplier(double multiplier) {
    if (multiplier < 1) {
      throw new IllegalArgumentException(
          "Invalid multiplier '"
              + multiplier
              + "'. Should be greater than "
              + "or equal to 1. A multiplier of 1 is equivalent to a fixed interval.");
    }
  }

  @Override
  public long nextBackOff(Object object, RqueueMessage rqueueMessage, int failureCount) {
    if (failureCount > getMaxRetries()) {
      return STOP;
    }
    return getDelay(failureCount);
  }

  protected long getDelay(int failureCount) {
    long maxDelay = (long) (failureCount * getMultiplier() * getInitialInterval());
    return Math.min(maxDelay, getMaxInterval());
  }
}
