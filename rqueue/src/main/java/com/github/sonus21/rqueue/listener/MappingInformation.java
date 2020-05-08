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

import static com.github.sonus21.rqueue.utils.Constants.DELTA_BETWEEN_RE_ENQUEUE_TIME;
import static com.github.sonus21.rqueue.utils.Constants.MIN_EXECUTION_TIME;

import com.github.sonus21.rqueue.models.MinMax;
import java.util.Map;
import java.util.Set;
import lombok.AccessLevel;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;

@SuppressWarnings("ComparableImplementedButEqualsNotOverridden")
@Getter(AccessLevel.PACKAGE)
@EqualsAndHashCode
@Builder
class MappingInformation implements Comparable<MappingInformation> {
  private Set<String> queueNames;
  private int numRetries;
  private String deadLetterQueueName;
  private long visibilityTimeout;
  private boolean active;
  private MinMax<Integer> concurrency;
  private String priorityGroup;
  private Map<String, Integer> priorities;

  @Override
  public int compareTo(MappingInformation o) {
    return 0;
  }

  @Override
  public String toString() {
    return String.join(", ", queueNames);
  }

  boolean isValid() {
    return active
        && getQueueNames().size() > 0
        && visibilityTimeout > MIN_EXECUTION_TIME + DELTA_BETWEEN_RE_ENQUEUE_TIME;
  }
}
