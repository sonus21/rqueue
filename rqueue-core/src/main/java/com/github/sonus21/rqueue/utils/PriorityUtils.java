/*
 * Copyright (c) 2019-2023 Sonu Kumar
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

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public final class PriorityUtils {

  private PriorityUtils() {
  }

  public static Set<String> getNamesFromPriority(String queueName, Map<String, Integer> priority) {
    Set<String> keys = new HashSet<>();
    for (String key : priority.keySet()) {
      if (!key.equals(Constants.DEFAULT_PRIORITY_KEY)) {
        keys.add(getQueueNameForPriority(queueName, key));
      }
    }
    return keys;
  }

  public static String getQueueNameForPriority(String queueName, String priority) {
    Validator.validateQueue(queueName);
    Validator.validatePriority(priority);
    return queueName + getSuffix(priority);
  }

  public static String getSuffix(String priority) {
    return "_" + priority;
  }
}
