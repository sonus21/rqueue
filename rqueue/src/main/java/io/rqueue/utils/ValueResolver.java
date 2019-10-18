package io.rqueue.utils;

import org.springframework.beans.factory.config.BeanExpressionContext;
import org.springframework.beans.factory.config.BeanExpressionResolver;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ConfigurableApplicationContext;

public abstract class ValueResolver {
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
    if (s.toLowerCase().equals("true")) {
      return true;
    } else if (s.toLowerCase().equals("false")) {
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
