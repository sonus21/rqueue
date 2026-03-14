package com.github.sonus21.rqueue.listener;

public class HardStrictPriorityPollerProperties {
  // set such default values for parameters "afterPollSleepInterval" and "semaphoreWaitTime"
  // local load tests have correct strict priority algorithm work and good performance with them
  private Long afterPollSleepInterval = 30L;
  private Long semaphoreWaitTime = 15L;

  public Long getAfterPollSleepInterval() {
    return afterPollSleepInterval;
  }

  public void setAfterPollSleepInterval(Long afterPollSleepInterval) {
    validateTimeInterval(afterPollSleepInterval);
    this.afterPollSleepInterval = afterPollSleepInterval;
  }

  public Long getSemaphoreWaitTime() {
    return this.semaphoreWaitTime;
  }

  public void setSemaphoreWaitTime(Long semaphoreWaitTime) {
    validateTimeInterval(semaphoreWaitTime);
    this.semaphoreWaitTime = semaphoreWaitTime;
  }

  private void validateTimeInterval(Long value) {
    if (value == null || value > 0) {
      return;
    }
    throw new IllegalArgumentException("Value must be positive: " + value);
  }
}
