/*
 * Copyright (c) 2020-2023 Sonu Kumar
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

package com.github.sonus21.rqueue.test.application;

import com.github.sonus21.rqueue.config.SimpleRqueueListenerContainerFactory;
import com.github.sonus21.rqueue.utils.backoff.ExponentialTaskExecutionBackOff;
import com.github.sonus21.rqueue.utils.backoff.FixedTaskExecutionBackOff;
import com.github.sonus21.rqueue.utils.backoff.TaskExecutionBackOff;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;

public abstract class BaseApplicationWithBackoff extends ApplicationBasicConfiguration {

  @Value("${backoff.class.name:fixed}")
  private String backOffClassName;

  @Value("${fixed.backoff.interval:-1}")
  private int fixedBackoffInterval;

  @Value("${exponential.backoff.initial.interval:-1}")
  private int exponentialBackoffInitialInterval;

  @PostConstruct
  public void postConstruct() {
    init();
  }

  @PreDestroy
  public void preDestroy() {
    cleanup();
  }

  @Bean
  public SimpleRqueueListenerContainerFactory simpleRqueueListenerContainerFactory() {
    TaskExecutionBackOff backOff;
    if (backOffClassName.equalsIgnoreCase("fixed")) {
      FixedTaskExecutionBackOff off = new FixedTaskExecutionBackOff();
      if (fixedBackoffInterval > 0) {
        off.setInterval(fixedBackoffInterval);
      }
      backOff = off;
    } else {
      ExponentialTaskExecutionBackOff off = new ExponentialTaskExecutionBackOff();
      off.setInitialInterval(exponentialBackoffInitialInterval);
      backOff = off;
    }
    SimpleRqueueListenerContainerFactory simpleRqueueListenerContainerFactory =
        new SimpleRqueueListenerContainerFactory();
    simpleRqueueListenerContainerFactory.setTaskExecutionBackOff(backOff);
    return simpleRqueueListenerContainerFactory;
  }
}
