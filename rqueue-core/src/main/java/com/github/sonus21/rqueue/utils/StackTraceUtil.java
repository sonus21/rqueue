package com.github.sonus21.rqueue.utils;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class StackTraceUtil {

  public void printStackTrace(int frameCount) {
    Thread thread = Thread.currentThread();
    StackTraceElement[] elements = thread.getStackTrace();
    int printed = 0;
    for (int i = 2; printed < frameCount && i < elements.length; i++) {
      StackTraceElement element = elements[i];
      String className = element.getClassName();
      if (className.startsWith("org.springframework.aop") ||
          className.startsWith("org.springframework.cglib.proxy")) {
        continue;
      }
      if (element.getMethodName().startsWith("invoke")) {
        continue;
      }
      log.info("{} {} {}", element.getClassName(), element.getMethodName(),
          element.getLineNumber());
      printed += 1;
    }
  }

}
