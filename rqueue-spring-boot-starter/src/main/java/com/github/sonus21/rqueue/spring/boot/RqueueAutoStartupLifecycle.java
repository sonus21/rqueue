/*
 * Copyright (c) 2026 Sonu Kumar
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

package com.github.sonus21.rqueue.spring.boot;

import com.github.sonus21.rqueue.listener.RqueueMessageListenerContainer;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.config.BeanPostProcessor;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.ApplicationListener;

/**
 * Delays Rqueue poller startup in Boot web applications until the servlet or reactive web server
 * is ready and Spring Boot has published {@link ApplicationReadyEvent}.
 */
public class RqueueAutoStartupLifecycle
    implements BeanPostProcessor, ApplicationListener<ApplicationReadyEvent> {

  private final Set<RqueueMessageListenerContainer> delayedContainers =
      ConcurrentHashMap.newKeySet();

  public void delayAutoStartup(RqueueMessageListenerContainer container) {
    container.setAutoStartup(false);
    delayedContainers.add(container);
  }

  @Override
  public Object postProcessBeforeInitialization(Object bean, String beanName)
      throws BeansException {
    if (bean instanceof RqueueMessageListenerContainer) {
      RqueueMessageListenerContainer container = (RqueueMessageListenerContainer) bean;
      if (container.isAutoStartup()) {
        delayAutoStartup(container);
      }
    }
    return bean;
  }

  @Override
  public void onApplicationEvent(ApplicationReadyEvent event) {
    for (RqueueMessageListenerContainer container : delayedContainers) {
      if (!container.isRunning()) {
        container.start();
      }
    }
  }
}
