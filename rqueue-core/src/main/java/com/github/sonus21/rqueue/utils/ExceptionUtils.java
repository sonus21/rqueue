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
