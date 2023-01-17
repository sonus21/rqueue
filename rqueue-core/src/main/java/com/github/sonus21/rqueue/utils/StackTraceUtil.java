/*
 * Copyright (c) 2022-2023 Sonu Kumar
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

import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public final class StackTraceUtil {

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
