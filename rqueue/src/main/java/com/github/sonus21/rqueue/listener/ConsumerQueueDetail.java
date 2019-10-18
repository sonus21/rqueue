package com.github.sonus21.rqueue.listener;

import static com.github.sonus21.rqueue.constants.Constants.QUEUE_NAME;

import com.github.sonus21.rqueue.constants.Constants;
import java.util.Collections;
import java.util.Map;

class ConsumerQueueDetail {
  private final String queueName;
  private final boolean delayedQueue;
  private final String dlqName;
  private final int numRetries;
  private final String zsetName;

  ConsumerQueueDetail(
      String queueName, int numRetries, String deadLaterQueueName, boolean delayedQueue) {
    this.queueName = queueName;
    this.numRetries = numRetries;
    this.delayedQueue = delayedQueue;
    this.dlqName = deadLaterQueueName;
    if (delayedQueue) {
      this.zsetName = Constants.getZsetName(queueName);
    } else {
      this.zsetName = null;
    }
  }

  Map<String, Object> getHeaders() {
    return Collections.singletonMap(QUEUE_NAME, queueName);
  }

  String getQueueName() {
    return queueName;
  }

  public boolean isDelayedQueue() {
    return delayedQueue;
  }

  String getDlqName() {
    return dlqName;
  }

  int getNumRetries() {
    return numRetries;
  }

  String getZsetName() {
    return zsetName;
  }
}
