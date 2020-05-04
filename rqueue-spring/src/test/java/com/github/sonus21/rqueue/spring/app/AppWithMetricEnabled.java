/*
 * Copyright 2019 Sonu Kumar
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

package com.github.sonus21.rqueue.spring.app;

import com.github.sonus21.rqueue.config.SimpleRqueueListenerContainerFactory;
import com.github.sonus21.rqueue.core.support.MessageProcessor;
import com.github.sonus21.rqueue.spring.EnableRqueue;
import com.github.sonus21.rqueue.spring.RqueueMetricsProperties;
import com.github.sonus21.rqueue.test.BaseApplication;
import io.micrometer.core.instrument.Tags;
import io.micrometer.prometheus.PrometheusConfig;
import io.micrometer.prometheus.PrometheusMeterRegistry;
import java.util.ArrayList;
import java.util.List;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.PropertySource;
import org.springframework.web.servlet.config.annotation.EnableWebMvc;

@Configuration
@ComponentScan(
    basePackages = {"com.github.sonus21.rqueue.test", "com.github.sonus21.rqueue.spring.services"})
@EnableRqueue
@EnableWebMvc
@PropertySource("classpath:application.properties")
public class AppWithMetricEnabled extends BaseApplication {
  @Value("${max.workers.count:6}")
  private int maxWorkers;

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

  public class DeleteMessageListener implements MessageProcessor {
    private List<Object> messages = new ArrayList<>();

    @Override
    public boolean process(Object message) {
      messages.add(message);
      return true;
    }

    public List<Object> getMessages() {
      return messages;
    }

    public void clear() {
      this.messages = new ArrayList<>();
    }
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
    return factory;
  }
}
