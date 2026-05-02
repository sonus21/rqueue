package com.github.sonus21.rqueue.enums;

public enum QueueType {
  /**
   * WorkQueue semantics: competing consumers, each message delivered to exactly one listener.
   */
  QUEUE,
  /**
   * Stream/fan-out semantics: every independent listener group receives all messages.
   */
  STREAM
}
