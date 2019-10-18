package io.rqueue.listener;

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
