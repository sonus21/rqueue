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

import java.beans.Introspector;

public class StringUtils {
  StringUtils() {}

  public static boolean isEmpty(String string) {
    if (string == null) {
      return true;
    }
    return string.isEmpty();
  }

  public static String clean(String string) {
    if (string == null) {
      return null;
    }
    return string.trim();
  }

  public static boolean isAlpha(Character c) {
    return Character.isUpperCase(c) || Character.isLowerCase(c);
  }

  public static String convertToCamelCase(String queueName) {
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < queueName.length(); i++) {
      Character c = queueName.charAt(i);
      if (isAlpha(c)) {
        if (i > 0 && !isAlpha(queueName.charAt(i - 1))) {
          sb.append(Character.toUpperCase(c));
        } else {
          sb.append(c);
        }
      }
    }
    return Introspector.decapitalize(sb.toString());
  }
}
