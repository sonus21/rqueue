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

package com.github.sonus21.rqueue.utils;

import static org.springframework.util.Assert.notNull;

public class Validator {
  private Validator() {}

  public static void validateMessage(Object message) {
    notNull(message, "message cannot be null");
  }

  public static void validateRetryCount(int retryCount) {
    if (retryCount < 0) {
      throw new IllegalArgumentException("retryCount must be positive");
    }
  }

  public static void validateDelay(long delayInMilliSecs) {
    if (delayInMilliSecs < 0) {
      throw new IllegalArgumentException("delayInMilliSecs must be positive");
    }
  }

  public static void validateQueue(String queue) {
    if (StringUtils.isEmpty(queue)) {
      throw new IllegalArgumentException("queue cannot be empty");
    }
  }

  public static void validatePriority(String priority) {
    if (StringUtils.isEmpty(priority)) {
      throw new IllegalArgumentException("priority cannot be empty");
    }
  }
}
