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

import org.springframework.beans.factory.config.BeanExpressionContext;
import org.springframework.beans.factory.config.BeanExpressionResolver;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ConfigurableApplicationContext;

public class ValueResolver {
  private ValueResolver() {}

  private static String[] wrapInStringArray(Object valueToWrap) {
    return new String[] {valueToWrap.toString()};
  }

  public static String[] resolveValueToArrayOfStrings(
      ApplicationContext applicationContext, String name) {
    if (applicationContext instanceof ConfigurableApplicationContext) {
      ConfigurableBeanFactory configurableBeanFactory =
          ((ConfigurableApplicationContext) applicationContext).getBeanFactory();
      String placeholdersResolved = configurableBeanFactory.resolveEmbeddedValue(name);
      BeanExpressionResolver exprResolver = configurableBeanFactory.getBeanExpressionResolver();
      if (exprResolver == null) {
        return wrapInStringArray(name);
      }
      Object result =
          exprResolver.evaluate(
              placeholdersResolved, new BeanExpressionContext(configurableBeanFactory, null));
      if (result instanceof String[]) {
        return (String[]) result;
      } else if (result != null) {
        return wrapInStringArray(result);
      } else {
        return wrapInStringArray(name);
      }
    }
    return wrapInStringArray(name);
  }

  public static Integer resolveValueToInteger(ApplicationContext applicationContext, String name) {
    if (applicationContext instanceof ConfigurableApplicationContext) {
      ConfigurableBeanFactory configurableBeanFactory =
          ((ConfigurableApplicationContext) applicationContext).getBeanFactory();
      String placeholdersResolved = configurableBeanFactory.resolveEmbeddedValue(name);
      BeanExpressionResolver exprResolver = configurableBeanFactory.getBeanExpressionResolver();
      if (exprResolver == null) {
        return Integer.parseInt(name);
      }
      Object result =
          exprResolver.evaluate(
              placeholdersResolved, new BeanExpressionContext(configurableBeanFactory, null));
      if (result instanceof Integer) {
        return (Integer) result;
      } else if (result instanceof String) {
        return Integer.parseInt((String) result);
      }
    }
    return Integer.parseInt(name);
  }

  public static boolean convertToBoolean(String s) {
    String tmpString = s.trim();
    if (tmpString.equalsIgnoreCase("true")) {
      return true;
    }
    if (tmpString.equalsIgnoreCase("false")) {
      return false;
    }
    throw new IllegalArgumentException(s + " can not be converted to boolean");
  }

  public static boolean resolveToBoolean(ApplicationContext applicationContext, String name) {
    if (applicationContext instanceof ConfigurableApplicationContext) {
      ConfigurableBeanFactory configurableBeanFactory =
          ((ConfigurableApplicationContext) applicationContext).getBeanFactory();
      String placeholdersResolved = configurableBeanFactory.resolveEmbeddedValue(name);
      BeanExpressionResolver exprResolver = configurableBeanFactory.getBeanExpressionResolver();
      if (exprResolver == null) {
        return convertToBoolean(name);
      }
      Object result =
          exprResolver.evaluate(
              placeholdersResolved, new BeanExpressionContext(configurableBeanFactory, null));
      if (result instanceof Boolean) {
        return (Boolean) result;
      } else if (result instanceof String) {
        return convertToBoolean((String) result);
      }
    }
    return convertToBoolean(name);
  }
}
