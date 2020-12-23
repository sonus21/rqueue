/*
 * Copyright 2020 Sonu Kumar
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.github.sonus21.rqueue.utils;

import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;

public class DateTimeUtils {
  private static final DateTimeFormatter simple = DateTimeFormatter.ofPattern("dd MMM yyyy HH:mm");

  DateTimeUtils() {}

  public static String milliToHumanRepresentation(long millis) {
    String prefix = "";
    long seconds = millis / Constants.ONE_MILLI;
    long minutes = seconds / Constants.SECONDS_IN_A_MINUTE;
    seconds = seconds % Constants.SECONDS_IN_A_MINUTE; // remaining seconds
    long hours = minutes / Constants.MINUTES_IN_AN_HOUR;
    minutes = minutes % Constants.MINUTES_IN_AN_HOUR; // remaining minutes
    long days = hours / Constants.HOURS_IN_A_DAY;
    hours = hours % Constants.HOURS_IN_A_DAY; // remaining hours
    String s;
    if (days != 0) {
      s = days + " Day, " + hours + " Hours, " + minutes + " Minutes, " + seconds + " Seconds.";
    } else {
      if (hours != 0) {
        s = hours + " Hours, " + minutes + " Minutes, " + seconds + " Seconds.";
      } else if (minutes != 0) {
        s = minutes + " Minutes, " + seconds + " Seconds.";
      } else {
        s = seconds + " Seconds.";
      }
    }
    return prefix + s;
  }

  public static String formatMilliToString(Long milli) {
    if (milli == null) {
      return "";
    }
    Instant instant = Instant.ofEpochMilli(milli);
    ZonedDateTime zonedDateTime = ZonedDateTime.ofInstant(instant, java.time.ZoneId.of("UTC"));
    return zonedDateTime.format(simple);
  }

  public static LocalDate localDateFromMilli(long millis) {
    return Instant.ofEpochMilli(millis).atZone(ZoneOffset.UTC).toLocalDate();
  }

  public static LocalDate today() {
    return LocalDate.now(ZoneOffset.UTC);
  }
}
