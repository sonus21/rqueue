/*
 * Copyright (c) 2020-2023 Sonu Kumar
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * You may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and limitations under the License.
 *
 */

package com.github.sonus21.rqueue.utils;

import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;

@NoArgsConstructor(access = AccessLevel.PRIVATE)
public final class DateTimeUtils {

  private static final DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ofPattern(
      "yyyy-MM-dd HH:mm");
  private static final DateTimeFormatter dateTimeFormatterWithSecond = DateTimeFormatter.ofPattern(
      "yyyy-MM-dd HH:mm:ss");

  private static String hourString(long hour) {
    if (hour > 1) {
      return hour + " Hours";
    }
    return hour + " Hour";
  }

  private static String minuteString(long minutes) {
    if (minutes > 1) {
      return minutes + " Minutes";
    }
    return minutes + " Minute";
  }

  private static String secondString(long seconds) {
    if (seconds > 1) {
      return seconds + " Seconds";
    }
    return seconds + " Second";
  }

  private static String dayString(long days) {
    if (days > 1) {
      return days + " Days";
    }
    return days + " Day";
  }

  private static String formatDay(long days, long hours, long minutes, long seconds) {
    if (hours == 0 && minutes == 0 && seconds == 0) {
      return dayString(days);
    }
    if (minutes == 0 && seconds == 0) {
      return String.format("%s, %s", dayString(days), hourString(hours));
    }
    if (seconds == 0) {
      return String.format("%s, %s, %s", dayString(days), hourString(hours), minuteString(minutes));
    }
    return String.format(
        "%s, %s, %s, %s",
        dayString(days), hourString(hours), minuteString(minutes), secondString(seconds));
  }

  private static String formatHour(long hours, long minutes, long seconds) {
    if (minutes == 0 && seconds == 0) {
      return hourString(hours);
    }
    if (seconds == 0) {
      return String.format("%s, %s", hourString(hours), minuteString(minutes));
    }
    return String.format(
        "%s, %s, %s", hourString(hours), minuteString(minutes), secondString(seconds));
  }

  public static String milliToHumanRepresentation(long millisecond) {
    long millis = millisecond;
    String prefix = "";
    if (millis < 0) {
      prefix = "- ";
      millis = -1 * millis;
    }
    long seconds = millis / Constants.ONE_MILLI;
    long minutes = seconds / Constants.SECONDS_IN_A_MINUTE;
    seconds = seconds % Constants.SECONDS_IN_A_MINUTE; // remaining seconds
    long hours = minutes / Constants.MINUTES_IN_AN_HOUR;
    minutes = minutes % Constants.MINUTES_IN_AN_HOUR; // remaining minutes
    long days = hours / Constants.HOURS_IN_A_DAY;
    hours = hours % Constants.HOURS_IN_A_DAY; // remaining hours
    if (days != 0) {
      return prefix + formatDay(days, hours, minutes, seconds);
    }
    if (hours != 0) {
      return prefix + formatHour(hours, minutes, seconds);
    }
    if (minutes != 0) {
      if (seconds == 0) {
        return prefix + minuteString(minutes);
      }
      return prefix + String.format("%s, %s", minuteString(minutes), secondString(seconds));
    }
    return prefix + secondString(seconds);
  }

  public static String formatMilliToString(Long milli) {
    if (milli == null) {
      return "";
    }
    Instant instant = Instant.ofEpochMilli(milli);
    ZonedDateTime zonedDateTime = ZonedDateTime.ofInstant(instant, ZoneId.systemDefault());
    return zonedDateTime.format(dateTimeFormatter);
  }

  public static LocalDate localDateFromMilli(long millis) {
    return Instant.ofEpochMilli(millis).atZone(ZoneId.systemDefault()).toLocalDate();
  }

  public static LocalDate today() {
    return LocalDate.now();
  }

  public static String currentTimeFormatted() {
    return LocalDateTime.now().format(dateTimeFormatterWithSecond);
  }
}
