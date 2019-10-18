package io.rqueue.core;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import java.io.Serializable;
import java.util.UUID;

@JsonIgnoreProperties(ignoreUnknown = true)
public class RqueueMessage implements Serializable {

  private static final long serialVersionUID = -3488860960637488519L;
  private String id;
  private String queueName;
  private String message;
  private Integer retryCount;
  private Long queuedTime;
  private long processAt;
  private Long accessTime;
  private Long reEnqueuedAt;

  public RqueueMessage() {}

  public RqueueMessage(String queueName, String message, Integer retryCount, Long delay) {
    this.queueName = queueName;
    this.message = message;
    this.retryCount = retryCount;
    this.queuedTime = System.currentTimeMillis();
    if (delay != null) {
      this.processAt = queuedTime + delay;
      this.id = queueName + UUID.randomUUID().toString();
    }
  }

  public void updateReEnqueuedAt() {
    this.reEnqueuedAt = System.currentTimeMillis();
  }

  public void updateAccessTime() {
    this.accessTime = System.currentTimeMillis();
  }

  public String getQueueName() {
    return this.queueName;
  }

  public void setQueueName(String queueName) {
    this.queueName = queueName;
  }

  public String getMessage() {
    return this.message;
  }

  public void setMessage(String message) {
    this.message = message;
  }

  public Integer getRetryCount() {
    return this.retryCount;
  }

  public void setRetryCount(Integer retryCount) {
    this.retryCount = retryCount;
  }

  public Long getQueuedTime() {
    return this.queuedTime;
  }

  public void setQueuedTime(Long queuedTime) {
    this.queuedTime = queuedTime;
  }

  public long getProcessAt() {
    return this.processAt;
  }

  public void setProcessAt(Long processAt) {
    this.processAt = processAt;
  }

  public Long getAccessTime() {
    return this.accessTime;
  }

  public void setAccessTime(Long accessTime) {
    this.accessTime = accessTime;
  }

  public Long getReEnqueuedAt() {
    return this.reEnqueuedAt;
  }

  public void setReEnqueuedAt(Long reEnqueuedAt) {
    this.reEnqueuedAt = reEnqueuedAt;
  }

  public String toString() {
    return "RqMessage(id="
        + this.getId()
        + "queueName="
        + this.getQueueName()
        + ", message="
        + this.getMessage()
        + ", retryCount="
        + this.getRetryCount()
        + ", queuedTime="
        + this.getQueuedTime()
        + ", processAt="
        + this.getProcessAt()
        + ", accessTime="
        + this.getAccessTime()
        + ", reEnqueuedAt="
        + this.getReEnqueuedAt()
        + ")";
  }

  public String getId() {
    return id;
  }

  public void setId(String id) {
    this.id = id;
  }
}
