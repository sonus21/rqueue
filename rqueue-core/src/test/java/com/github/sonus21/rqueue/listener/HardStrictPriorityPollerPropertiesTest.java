package com.github.sonus21.rqueue.listener;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.stream.LongStream;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.NullSource;

class HardStrictPriorityPollerPropertiesTest {

  private final HardStrictPriorityPollerProperties properties =
      new HardStrictPriorityPollerProperties();

  static LongStream invalidValues() {
    return LongStream.of(0L, -1L, -100L, Long.MIN_VALUE);
  }

  static LongStream validValues() {
    return LongStream.of(1L, Long.MAX_VALUE);
  }

  @ParameterizedTest
  @MethodSource("invalidValues")
  void setAfterPollSleepIntervalInvalidValues(Long value) {
    assertThrows(IllegalArgumentException.class, () -> properties.setAfterPollSleepInterval(value));
  }

  @ParameterizedTest
  @NullSource
  @MethodSource("validValues")
  void setAfterPollSleepIntervalValidValues(Long value) {
    assertDoesNotThrow(() -> properties.setAfterPollSleepInterval(value));
  }

  @ParameterizedTest
  @MethodSource("invalidValues")
  void setSemaphoreWaitTimeInvalidValues(Long value) {
    assertThrows(IllegalArgumentException.class, () -> properties.setSemaphoreWaitTime(value));
  }

  @ParameterizedTest
  @NullSource
  @MethodSource("validValues")
  void setSemaphoreWaitTimeValidValues(Long value) {
    assertDoesNotThrow(() -> properties.setSemaphoreWaitTime(value));
  }
}
