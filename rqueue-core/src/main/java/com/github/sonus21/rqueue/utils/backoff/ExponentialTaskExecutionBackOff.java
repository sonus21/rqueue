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
 * Implementation of the {@link TaskExecutionBackOff} class, that increases the delay exponentially
 * at the specified rate.
 *
 * <p>The delay is defined as Math.pow(multiplier, failureCount) * initialInterval.
 *
 * <p>Default delay for the task is 1.5 seconds, that increases at the rate of 1.5 on each failure.
 * After a hundred plus failures, it will reach the limit of long value {@value Long#MAX_VALUE}, and
 * no more retry would be possible, by default any task can be delay upto 3 hours
 * {@value #DEFAULT_MAX_INTERVAL}.
 *
 * <p>Using multiplier 1 would mean using the fixed execution back off {@link
 * FixedTaskExecutionBackOff}
 */
public class ExponentialTaskExecutionBackOff implements TaskExecutionBackOff {

  /**
   * The default initial delay for the any task: 1500 ms (1.5 seconds)
   */
  public static final long DEFAULT_INITIAL_INTERVAL = 1500L;

  /**
   * The default multiplier (increases the delay by 50%).
   */
  public static final double DEFAULT_MULTIPLIER = 1.5;

  /**
   * The default maximum delay for any task (3 hours)
   */
  public static final long DEFAULT_MAX_INTERVAL = 10800000L;

  private long initialInterval = DEFAULT_INITIAL_INTERVAL;
  private long maxInterval = DEFAULT_MAX_INTERVAL;
  private double multiplier = DEFAULT_MULTIPLIER;

  private int maxRetries = Integer.MAX_VALUE;

  /**
   * Create an instance with an initial interval of {@value #DEFAULT_INITIAL_INTERVAL} ms, maximum
   * interval of {@value #DEFAULT_MAX_INTERVAL}, multiplier of {@value #DEFAULT_MULTIPLIER} and
   * default unlimited retries.
   */
  public ExponentialTaskExecutionBackOff() {
  }

  /**
   * Create an instance.
   *
   * @param initialInterval the initial delay for the message
   * @param maxInterval     the maximum delay for the message.
   * @param multiplier      the factor at which delay would be increased.
   * @param maxRetries      the maximum retry count for any message.
   */
  public ExponentialTaskExecutionBackOff(
      long initialInterval, long maxInterval, double multiplier, int maxRetries) {
    checkInitialInterval(initialInterval);
    checkMaxInterval(initialInterval, maxInterval);
    checkMultiplier(multiplier);
    checkMaxRetries(maxRetries);
    this.initialInterval = initialInterval;
    this.maxInterval = maxInterval;
    this.multiplier = multiplier;
    this.maxRetries = maxRetries;
  }

  private void checkInitialInterval(long initialInterval) {
    if (initialInterval <= 0) {
      throw new IllegalArgumentException("initialInterval must be > 0");
    }
  }

  private void checkMaxRetries(int maxRetries) {
    if (maxRetries < 0) {
      throw new IllegalArgumentException("maxRetries must be > 0");
    }
  }

  private void checkMaxInterval(long initialInterval, long maxInterval) {
    if (maxInterval < initialInterval) {
      throw new IllegalArgumentException(
          "maxInterval must be greater than or equal to initialInterval");
    }
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

  /**
   * Return the initial interval in milliseconds.
   *
   * @return configured initial interval.
   */
  public long getInitialInterval() {
    return this.initialInterval;
  }

  /**
   * Set the initial interval in milliseconds.
   *
   * @param initialInterval initial interval in milliseconds.
   */
  public void setInitialInterval(long initialInterval) {
    checkInitialInterval(initialInterval);
    this.initialInterval = initialInterval;
  }

  /**
   * Return the maximum number of retries.
   *
   * @return maximum retries that will be performed.
   */
  public int getMaxRetries() {
    return this.maxRetries;
  }

  /**
   * Set the maximum number of retries
   *
   * @param maxRetries maximum retries
   */
  public void setMaxRetries(int maxRetries) {
    checkMaxRetries(maxRetries);
    this.maxRetries = maxRetries;
  }

  /**
   * get the multiplier
   *
   * @return multiplier for this back off.
   */
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
   * Set the max interval in milliseconds
   *
   * @param maxInterval value in milliseconds
   */
  public void setMaxInterval(long maxInterval) {
    checkMaxInterval(initialInterval, maxInterval);
    this.maxInterval = maxInterval;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public long nextBackOff(Object message, RqueueMessage rqueueMessage, int failureCount) {
    if (failureCount >= getMaxRetries(message, rqueueMessage, failureCount)) {
      return STOP;
    }
    return getDelay(message, rqueueMessage, failureCount);
  }

  protected int getMaxRetries(Object message, RqueueMessage rqueueMessage, int failureCount) {
    return getMaxRetries();
  }

  protected long getDelay(Object message, RqueueMessage rqueueMessage, int failureCount) {
    return getDelay(failureCount);
  }

  protected long getDelay(int failureCount) {
    long maxDelay = (long) (Math.pow(getMultiplier(), failureCount) * getInitialInterval());
    if (maxDelay == Long.MAX_VALUE) {
      return STOP;
    }
    return Math.min(maxDelay, getMaxInterval());
  }
}
