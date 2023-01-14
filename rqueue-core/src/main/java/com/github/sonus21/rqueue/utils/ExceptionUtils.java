/*
 * Copyright (c) 2021-2023 Sonu Kumar
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

import java.io.PrintWriter;
import java.io.StringWriter;

public final class ExceptionUtils {

  private ExceptionUtils() {
  }

  public static String getTraceback(Throwable e, int maxLength) {
    if (e == null) {
      return null;
    }
    StringWriter sw = new StringWriter();
    PrintWriter pw = new PrintWriter(sw);
    e.printStackTrace(pw);
    String stacktrace = sw.toString();
    if (stacktrace.length() > maxLength + 3) {
      stacktrace = stacktrace.substring(0, maxLength) + "...";
    }
    return stacktrace;
  }
}
