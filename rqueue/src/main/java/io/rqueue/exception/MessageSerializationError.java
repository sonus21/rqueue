package io.rqueue.exception;

public class MessageSerializationError extends RuntimeException {
  public MessageSerializationError(String message, Throwable cause) {
    super(message, cause);
  }
}
