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

package com.github.sonus21.rqueue.utils;

import com.github.sonus21.rqueue.exception.TimedOutException;
import java.util.function.BooleanSupplier;

/**
 * waitFor method wait for some event to occur. It accepts a callback as well that can be invoked on
 * successive failure of the method. That can be used to diagnosis the test. A callback method is
 * used to identify the outcome of some event, if the specified method returns true then call is
 * stopped otherwise every 100Ms the callback would be called to get the outcome. It tries for
 * continuously over 10seconds If callback does not return true within 10 seconds then it will throw
 * TimeOutException. If postmortem method is provided then it will call that method before throwing
 * exception.
 */
public final class TimeoutUtils {

  public static final long EXECUTION_TIME = 10_000L;
  public static final long SLEEP_TIME = 100L;

  private TimeoutUtils() {
  }

  public static void waitFor(
      BooleanSupplier callback, long waitTimeInMilliSeconds, String description)
      throws TimedOutException {
    waitFor(callback, waitTimeInMilliSeconds, SLEEP_TIME, description, () -> {
    });
  }

  public static void waitFor(BooleanSupplier callback, String description)
      throws TimedOutException {
    waitFor(callback, EXECUTION_TIME, description);
  }

  public static void waitFor(BooleanSupplier callback, String description, Runnable postmortem)
      throws TimedOutException {
    waitFor(callback, EXECUTION_TIME, SLEEP_TIME, description, postmortem);
  }

  public static void waitFor(
      BooleanSupplier callback,
      long waitTimeInMilliSeconds,
      String description,
      Runnable postmortem)
      throws TimedOutException {
    waitFor(callback, waitTimeInMilliSeconds, SLEEP_TIME, description, postmortem);
  }

  public static void waitFor(
      BooleanSupplier callback,
      long waitTimeInMilliSeconds,
      long sleepDuration,
      String description,
      Runnable postmortem)
      throws TimedOutException {
    long endTime = System.currentTimeMillis() + waitTimeInMilliSeconds;
    do {
      if (Boolean.TRUE.equals(callback.getAsBoolean())) {
        return;
      }
      sleep(sleepDuration);
    } while (System.currentTimeMillis() < endTime);
    try {
      postmortem.run();
    } catch (Exception e) {
      e.printStackTrace();
    }
    throw new TimedOutException("Timed out waiting for " + description);
  }

  public static void sleep(long time) {
    sleepLog(time, true);
  }

  public static void sleepLog(long time, boolean log) {
    try {
      Thread.sleep(time);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      if (log) {
        e.printStackTrace();
      }
    }
  }
}
