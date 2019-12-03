/*
 * Copyright (c) 2019-2019, Sonu Kumar
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

import com.github.sonus21.rqueue.exception.TimedOutException;
import java.util.function.Supplier;

public abstract class TimeUtil {
  public static void waitFor(
      Supplier<Boolean> callback, long waitTimeInMilliSeconds, String description)
      throws TimedOutException {
    long endTime = System.currentTimeMillis() + waitTimeInMilliSeconds;
    do {
      if (Boolean.TRUE.equals(callback.get())) {
        return;
      }
      try {
        Thread.sleep(100L);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    } while (System.currentTimeMillis() < endTime);
    throw new TimedOutException("Timed out waiting for " + description);
  }

  public static void waitFor(Supplier<Boolean> callback, String description)
      throws TimedOutException {
    waitFor(callback, 10000L, description);
  }
}
