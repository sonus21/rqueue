package com.github.sonus21.rqueue.constants;

public abstract class Constants {
  private static final String DELAYED_QUEUE_PREFIX = "rqueue-delay";
  public static final String QUEUE_NAME = "QUEUE_NAME";

  public static String getZsetName(String queueName) {
    return DELAYED_QUEUE_PREFIX + queueName;
  }
}
