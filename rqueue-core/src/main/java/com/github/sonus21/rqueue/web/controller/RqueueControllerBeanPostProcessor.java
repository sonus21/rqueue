/*
 *  Copyright 2021 Sonu Kumar
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *         https://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and limitations under the License.
 *
 */

package com.github.sonus21.rqueue.web.controller;

import static com.github.sonus21.rqueue.utils.Constants.FORWARD_SLASH;

import com.github.sonus21.rqueue.utils.StringUtils;
import java.lang.annotation.Annotation;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.config.BeanPostProcessor;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.core.annotation.AnnotationUtils;
import org.springframework.stereotype.Component;
import org.springframework.web.bind.annotation.RequestMapping;

@Slf4j
@Component
public class RqueueControllerBeanPostProcessor
    implements BeanPostProcessor, ApplicationContextAware {

  private static final String ANNOTATION_METHOD = "annotationData";
  private static final String DECLARED_ANNOTATIONS = "declaredAnnotations";
  private static final String WEB_URL_PREFIX = "rqueue.web.url.prefix";
  private ApplicationContext applicationContext;

  private String getPrefix() {
    String apiPrefix = applicationContext.getEnvironment().getProperty(WEB_URL_PREFIX, "");
    if (StringUtils.isEmpty(apiPrefix)) {
      return apiPrefix;
    }
    if (!apiPrefix.endsWith(FORWARD_SLASH)) {
      apiPrefix = apiPrefix + FORWARD_SLASH;
    }
    if (!apiPrefix.startsWith(FORWARD_SLASH)) {
      apiPrefix = FORWARD_SLASH + apiPrefix;
    }
    return apiPrefix;
  }

  @Override
  public Object postProcessAfterInitialization(Object bean, String beanName) throws BeansException {
    RRestController rRestController =
        AnnotationUtils.findAnnotation(bean.getClass(), RRestController.class);
    RViewController rViewController = null;
    if (rRestController == null) {
      rViewController = AnnotationUtils.findAnnotation(bean.getClass(), RViewController.class);
    }
    boolean annotated = (null != rViewController) || (null != rRestController);
    if (annotated) {
      RequestMapping requestMapping =
          AnnotationUtils.findAnnotation(bean.getClass(), RequestMapping.class);
      if (null != requestMapping) {
        Map<String, Object> attributes = AnnotationUtils.getAnnotationAttributes(requestMapping);
        String[] values = (String[]) attributes.get("path");
        String prefix = getPrefix();
        if (values.length > 0) {
          values = Arrays.stream(values).map(e -> prefix + e).toArray(String[]::new);
          attributes.put("value", values);
          attributes.put("path", values);
          RequestMapping target =
              AnnotationUtils.synthesizeAnnotation(attributes, RequestMapping.class, null);
          changeAnnotationValue(bean.getClass(), target);
        }
      }
    }
    return bean;
  }

  @SuppressWarnings("unchecked")
  private static void changeAnnotationValue(Class<?> targetClass, Annotation targetValue) {
    try {
      Method method = Class.class.getDeclaredMethod(ANNOTATION_METHOD);
      Object annotationData = method.invoke(targetClass);
      Field annotations = annotationData.getClass().getDeclaredField(DECLARED_ANNOTATIONS);
      Map<Class<? extends Annotation>, Annotation> map =
          (Map<Class<? extends Annotation>, Annotation>) annotations.get(annotationData);
      map.put(RequestMapping.class, targetValue);
    } catch (Exception e) {
      log.error("Error changing annotation", e);
    }
  }

  @Override
  public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
    this.applicationContext = applicationContext;
  }
}
