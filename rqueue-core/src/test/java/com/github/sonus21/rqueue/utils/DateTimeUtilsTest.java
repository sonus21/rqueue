/*
 *  Copyright 2021 Sonu Kumar
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *         https://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and limitations under the License.
 *
 */

package com.github.sonus21.rqueue.utils;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.github.sonus21.TestBase;
import com.github.sonus21.rqueue.CoreUnitTest;
import org.junit.jupiter.api.Test;

@CoreUnitTest
class DateTimeUtilsTest extends TestBase {

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
  void negativeMilliToHumanRepresentation() {
    assertEquals("- 1 Second", DateTimeUtils.milliToHumanRepresentation(-1000));
    assertEquals("- 30 Seconds", DateTimeUtils.milliToHumanRepresentation(-30000));

    assertEquals("- 2 Minutes", DateTimeUtils.milliToHumanRepresentation(-120000));
    assertEquals("- 2 Minutes, 1 Second", DateTimeUtils.milliToHumanRepresentation(-121000));
    assertEquals("- 1 Minute, 30 Seconds", DateTimeUtils.milliToHumanRepresentation(-90000));

    assertEquals("- 2 Hours", DateTimeUtils.milliToHumanRepresentation(-7200000));
    assertEquals("- 1 Hour", DateTimeUtils.milliToHumanRepresentation(-3600000));
    assertEquals("- 2 Hours, 2 Minutes", DateTimeUtils.milliToHumanRepresentation(-7320000));
    assertEquals(
        "- 2 Hours, 2 Minutes, 1 Second", DateTimeUtils.milliToHumanRepresentation(-7321000));

    assertEquals("- 1 Day", DateTimeUtils.milliToHumanRepresentation(-86400000));
    assertEquals("- 2 Days", DateTimeUtils.milliToHumanRepresentation(-172800000));
    assertEquals("- 2 Days, 2 Hours", DateTimeUtils.milliToHumanRepresentation(-180000000));
    assertEquals(
        "- 2 Days, 2 Hours, 2 Minutes", DateTimeUtils.milliToHumanRepresentation(-180120000));
    assertEquals(
        "- 2 Days, 2 Hours, 2 Minutes, 1 Second",
        DateTimeUtils.milliToHumanRepresentation(-180121000));
  }

  @Test
  void formatMilliToString() {
    assertEquals("1970-04-26 17:46", DateTimeUtils.formatMilliToString(10000000000L));
    assertEquals("", DateTimeUtils.formatMilliToString(null));
  }
}
