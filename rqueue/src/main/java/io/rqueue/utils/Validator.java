package io.rqueue.utils;

import java.util.List;

public abstract class Validator {
  public static void validateQueueNameAndMessage(String queueName, Object message) {
    if (queueName == null) {
      throw new IllegalArgumentException("queueName can not be null");
    }
    if (message == null) {
      throw new IllegalArgumentException("message can  not be null");
    }
  }

  public static void validateQueueNameAndMessages(String queueName, List<Object> messages) {
    if (queueName == null) {
      throw new IllegalArgumentException("queueName can not be null");
    }
    if (messages == null) {
      throw new IllegalArgumentException("messages can  not be null");
    }
    if (messages.isEmpty()) {
      throw new IllegalArgumentException("messages list is empty");
    }
  }
}
