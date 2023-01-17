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

package com.github.sonus21.rqueue.spring.app;

import com.github.sonus21.rqueue.config.SimpleRqueueListenerContainerFactory;
import com.github.sonus21.rqueue.models.enums.PriorityMode;
import com.github.sonus21.rqueue.spring.EnableRqueue;
import com.github.sonus21.rqueue.spring.RqueueMetricsProperties;
import com.github.sonus21.rqueue.test.DeleteMessageListener;
import com.github.sonus21.rqueue.test.application.BaseApplication;
import io.micrometer.core.instrument.Tags;
import io.micrometer.prometheus.PrometheusConfig;
import io.micrometer.prometheus.PrometheusMeterRegistry;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.PropertySource;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.web.servlet.config.annotation.EnableWebMvc;

@Configuration
@ComponentScan(
    basePackages = {"com.github.sonus21.rqueue.test", "com.github.sonus21.rqueue.spring.services"})
@EnableRqueue
@EnableWebMvc
@PropertySource("classpath:application.properties")
public class SpringApp extends BaseApplication {

  @Value("${max.workers.count:6}")
  private int maxWorkers;

  @Value("${priority.mode:}")
  private PriorityMode priorityMode;

  @Value("${provide.executor:false}")
  private boolean provideExecutor;

  @Bean
  public PrometheusMeterRegistry meterRegistry() {
    return new PrometheusMeterRegistry(PrometheusConfig.DEFAULT);
  }

  @Bean
  public RqueueMetricsProperties rqueueMetricsProperties() {
    RqueueMetricsProperties metricsProperties = new RqueueMetricsProperties();
    metricsProperties.setMetricTags(Tags.of("rqueue", "test"));
    metricsProperties.getCount().setExecution(true);
    metricsProperties.getCount().setFailure(true);
    return metricsProperties;
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
