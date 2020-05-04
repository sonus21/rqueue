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

import java.util.Set;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;

@SuppressWarnings("ComparableImplementedButEqualsNotOverridden")
@Getter(AccessLevel.PACKAGE)
@AllArgsConstructor
@EqualsAndHashCode
class MappingInformation implements Comparable<MappingInformation> {
  private final Set<String> queueNames;
  private final boolean delayedQueue;
  private final int numRetries;
  private final String deadLetterQueueName;
  private final long visibilityTimeout;
  private final boolean active;

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
