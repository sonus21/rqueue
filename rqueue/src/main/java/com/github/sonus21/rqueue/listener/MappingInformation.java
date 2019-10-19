/*
 * Copyright (c)  2019-2019, Sonu Kumar
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *       https://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */

package com.github.sonus21.rqueue.listener;

import java.util.Collections;
import java.util.Set;

@SuppressWarnings("ComparableImplementedButEqualsNotOverridden")
class MappingInformation implements Comparable<MappingInformation> {
  private final Set<String> queueNames;
  private int numRetries;
  private boolean delayedQueue;
  private String deadLaterQueueName;

  MappingInformation(
      Set<String> queueNames, boolean delayedQueue, int numRetries, String deadLaterQueueName) {
    this.queueNames = Collections.unmodifiableSet(queueNames);
    this.delayedQueue = delayedQueue;
    this.numRetries = numRetries;
    this.deadLaterQueueName = deadLaterQueueName;
  }

  Set<String> getQueueNames() {
    return this.queueNames;
  }

  @SuppressWarnings("NullableProblems")
  @Override
  public int compareTo(MappingInformation o) {
    return 0;
  }

  @Override
  public String toString() {
    return String.join(", ", queueNames);
  }

  public int getNumRetries() {
    return numRetries;
  }

  public boolean isDelayedQueue() {
    return delayedQueue;
  }

  public String getDeadLaterQueueName() {
    return deadLaterQueueName;
  }

  boolean isValid() {
    return getQueueNames().size() > 0;
  }
}
