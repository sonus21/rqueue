/*
 * Copyright (c) 2019-2023 Sonu Kumar
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

import static com.github.sonus21.rqueue.utils.StringUtils.clean;

import org.springframework.beans.factory.config.BeanExpressionContext;
import org.springframework.beans.factory.config.BeanExpressionResolver;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.expression.Expression;
import org.springframework.expression.ExpressionParser;
import org.springframework.expression.spel.standard.SpelExpressionParser;
import org.springframework.lang.NonNull;

public final class ValueResolver {

  private ValueResolver() {
  }

  @SuppressWarnings("unchecked")
  public static <T> T parseStringUsingSpel(String val, Class<?> t) {
    ExpressionParser parser = new SpelExpressionParser();
    Expression exp = parser.parseExpression(val);
    return (T) exp.getValue(t);
  }

  public static Long parseStringToLong(String val) {
    if (val == null) {
      return null;
    }
    String tmpVal = clean(val);
    if (tmpVal.equals("null")) {
      return null;
    }
    return parseStringUsingSpel(val, Long.class);
  }

  public static Integer parseStringToInt(String val) {
    if (val == null) {
      return null;
    }
    String tmpVal = clean(val);
    if (tmpVal.equals("null") || tmpVal.isEmpty()) {
      return null;
    }
    return parseStringUsingSpel(val, Integer.class);
  }

  public static boolean convertToBoolean(String s) {
    String tmpString = clean(s);
    if (tmpString == null) {
      return false;
    }
    if (tmpString.equalsIgnoreCase("true")
        || tmpString.equals("1")
        || tmpString.equalsIgnoreCase("yes")) {
      return true;
    }
    if (tmpString.equalsIgnoreCase("false")
        || tmpString.equals("0")
        || tmpString.equals("")
        || tmpString.equalsIgnoreCase("no")) {
      return false;
    }
    throw new IllegalArgumentException(s + " cannot be converted to boolean");
  }

  @NonNull
  private static Object resolveExpression(ApplicationContext applicationContext, String name) {
    if (applicationContext instanceof ConfigurableApplicationContext) {
      ConfigurableBeanFactory configurableBeanFactory =
          ((ConfigurableApplicationContext) applicationContext).getBeanFactory();
      String placeholdersResolved = configurableBeanFactory.resolveEmbeddedValue(name);
      BeanExpressionResolver exprResolver = configurableBeanFactory.getBeanExpressionResolver();
      if (exprResolver == null) {
        return name;
      }
      Object result =
          exprResolver.evaluate(
              placeholdersResolved, new BeanExpressionContext(configurableBeanFactory, null));
      if (result != null) {
        return result;
      }
    }
    return name;
  }

  public static String[] resolveKeyToArrayOfStrings(
      ApplicationContext applicationContext, String name) {
    Object result = resolveExpression(applicationContext, name);
    String[] values;
    if (result instanceof String[]) {
      values = (String[]) result;
    } else {
      values = ((String) result).split(Constants.Comma);
    }
    String[] cleanedStrings = new String[values.length];
    for (int i = 0; i < values.length; i++) {
      cleanedStrings[i] = clean(values[i]);
    }
    return cleanedStrings;
  }

  public static String resolveKeyToString(ApplicationContext applicationContext, String name) {
    String[] values = resolveKeyToArrayOfStrings(applicationContext, name);
    if (values.length == 1) {
      return values[0];
    }
    throw new IllegalArgumentException("More than one value provided");
  }

  public static Integer resolveKeyToInteger(ApplicationContext applicationContext, String name) {
    Object result = resolveExpression(applicationContext, name);
    if (result instanceof Integer) {
      return (Integer) result;
    }
    return parseStringToInt((String) result);
  }

  public static Long resolveKeyToLong(ApplicationContext applicationContext, String name) {
    Object result = resolveExpression(applicationContext, name);
    if (result instanceof Long) {
      return (Long) result;
    } else if (result instanceof Integer) {
      return ((Integer) result).longValue();
    }
    return parseStringToLong((String) result);
  }

  public static boolean resolveToBoolean(ApplicationContext applicationContext, String name) {
    Object result = resolveExpression(applicationContext, name);
    if (result instanceof Boolean) {
      return (Boolean) result;
    }
    return convertToBoolean((String) result);
  }
}
