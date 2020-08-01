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

@SuppressWarnings("WeakerAccess")
public final class StringUtils {
  private StringUtils() {}

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

  public static String getBeanName(String queueName) {
    return convertToCamelCase(queueName);
  }

  public static String convertToCamelCase(String string) {
    String txt = clean(string);
    if (isEmpty(txt)) {
      throw new IllegalArgumentException("string is empty");
    }
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < txt.length(); i++) {
      char c = txt.charAt(i);
      if (isAlpha(c)) {
        if (i == 0) {
          sb.append(c);
        } else if (!isAlpha(txt.charAt(i - 1))) {
          sb.append(Character.toUpperCase(c));
        } else if (Character.isLowerCase(c)
            || (Character.isUpperCase(c) && Character.isLowerCase(txt.charAt(i - 1)))) {
          sb.append(c);
        } else {
          sb.append(Character.toLowerCase(c));
        }
      }
    }
    String convertedTxt = sb.toString();
    if (convertedTxt.isEmpty()) {
      return txt;
    }
    return Introspector.decapitalize(convertedTxt);
  }

  public static String groupName(String name) {
    String groupName = convertToCamelCase(name);
    if (isAlpha(groupName.charAt(0)) && Character.isLowerCase(groupName.charAt(0))) {
      char[] chars = groupName.toCharArray();
      chars[0] = Character.toUpperCase(chars[0]);
      return new String(chars);
    }
    return groupName;
  }
}
