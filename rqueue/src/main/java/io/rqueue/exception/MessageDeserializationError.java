package io.rqueue.exception;

public class MessageDeserializationError extends RuntimeException {
  public MessageDeserializationError(String message, Throwable cause) {
    super(message, cause);
  }
}
