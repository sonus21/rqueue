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

package com.github.sonus21.rqueue.spring.boot.reactive;

import com.github.sonus21.rqueue.config.SimpleRqueueListenerContainerFactory;
import com.github.sonus21.rqueue.models.enums.PriorityMode;
import com.github.sonus21.rqueue.spring.boot.application.Application;
import com.github.sonus21.rqueue.test.DeleteMessageListener;
import com.github.sonus21.rqueue.test.application.BaseApplication;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.PropertySource;
import org.springframework.data.jpa.repository.config.EnableJpaRepositories;
import org.springframework.data.redis.repository.configuration.EnableRedisRepositories;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.transaction.annotation.EnableTransactionManagement;

@PropertySource("classpath:application.properties")
@SpringBootApplication(scanBasePackages = {"com.github.sonus21.rqueue.test"})
@EnableRedisRepositories
@EnableJpaRepositories(basePackages = {"com.github.sonus21.rqueue.test.repository"})
@EnableTransactionManagement
public class ReactiveWebApplication extends BaseApplication {

  @Value("${max.workers.count:6}")
  private int maxWorkers;

  @Value("${priority.mode:}")
  private PriorityMode priorityMode;

  @Value("${provide.executor:false}")
  private boolean provideExecutor;

  public static void main(String[] args) {
    SpringApplication.run(Application.class, args);
  }

  @Bean
  public DeleteMessageListener deleteMessageListener() {
    return new DeleteMessageListener();
  }

  @Bean
  public SimpleRqueueListenerContainerFactory simpleRqueueListenerContainerFactory(
      DeleteMessageListener deleteMessageListener) {
    SimpleRqueueListenerContainerFactory factory = new SimpleRqueueListenerContainerFactory();
    factory.setMaxNumWorkers(maxWorkers);
    factory.setManualDeletionMessageProcessor(deleteMessageListener);
    if (priorityMode != null) {
      factory.setPriorityMode(priorityMode);
    }
    if (provideExecutor) {
      ThreadPoolTaskExecutor threadPoolTaskExecutor = new ThreadPoolTaskExecutor();
      threadPoolTaskExecutor.setCorePoolSize(maxWorkers);
      threadPoolTaskExecutor.afterPropertiesSet();
      factory.setTaskExecutor(threadPoolTaskExecutor);
    }
    return factory;
  }
}
