package com.github.sonus21.rqueue.utils;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.github.sonus21.rqueue.CoreUnitTest;
import org.junit.jupiter.api.Test;

@CoreUnitTest
class DateTimeUtilsTest {

  @Test
  void milliToHumanRepresentation() {
    assertEquals("1 Second", DateTimeUtils.milliToHumanRepresentation(1000));
    assertEquals("30 Seconds", DateTimeUtils.milliToHumanRepresentation(30000));

    assertEquals("2 Minutes", DateTimeUtils.milliToHumanRepresentation(120000));
    assertEquals("2 Minutes, 1 Second", DateTimeUtils.milliToHumanRepresentation(121000));
    assertEquals("1 Minute, 30 Seconds", DateTimeUtils.milliToHumanRepresentation(90000));

    assertEquals("2 Hours", DateTimeUtils.milliToHumanRepresentation(7200000));
    assertEquals("1 Hour", DateTimeUtils.milliToHumanRepresentation(3600000));
    assertEquals("2 Hours, 2 Minutes", DateTimeUtils.milliToHumanRepresentation(7320000));
    assertEquals("2 Hours, 2 Minutes, 1 Second", DateTimeUtils.milliToHumanRepresentation(7321000));

    assertEquals("1 Day", DateTimeUtils.milliToHumanRepresentation(86400000));
    assertEquals("2 Days", DateTimeUtils.milliToHumanRepresentation(172800000));
    assertEquals("2 Days, 2 Hours", DateTimeUtils.milliToHumanRepresentation(180000000));
    assertEquals("2 Days, 2 Hours, 2 Minutes", DateTimeUtils.milliToHumanRepresentation(180120000));
    assertEquals(
        "2 Days, 2 Hours, 2 Minutes, 1 Second",
        DateTimeUtils.milliToHumanRepresentation(180121000));
  }

  @Test
  void formatMilliToString() {
    assertEquals("26 Apr 1970 17:46", DateTimeUtils.formatMilliToString(10000000000L));
    assertEquals("", DateTimeUtils.formatMilliToString(null));
  }
}
