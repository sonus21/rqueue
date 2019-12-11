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

package com.github.sonus21.rqueue.spring.boot;

import com.github.sonus21.rqueue.listener.RqueueMessageListenerContainer;
import com.github.sonus21.rqueue.metrics.RqueueCounter;
import com.github.sonus21.rqueue.metrics.RqueueMetrics;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tags;
import java.util.Map.Entry;
import org.springframework.boot.actuate.autoconfigure.metrics.MetricsAutoConfiguration;
import org.springframework.boot.actuate.autoconfigure.metrics.MetricsProperties;
import org.springframework.boot.autoconfigure.AutoConfigureAfter;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration(proxyBeanMethods = false)
@ConditionalOnClass({MeterRegistry.class})
@AutoConfigureAfter(MetricsAutoConfiguration.class)
@EnableConfigurationProperties(RqueueMetricsProperties.class)
public class RqueueMetricsAutoConfig {
  @Bean
  public RqueueCounter rqueueCounter(
      MetricsProperties metricsProperties,
      MeterRegistry meterRegistry,
      RqueueMessageListenerContainer rqueueMessageListenerContainer,
      RqueueMetricsProperties rqueueMetricsProperties) {
    Tags actualTags = Tags.empty();
    for (Entry<String, String> e : metricsProperties.getTags().entrySet()) {
      actualTags = Tags.concat(actualTags, e.getKey(), e.getValue());
    }
    for (Entry<String, String> e : rqueueMetricsProperties.getTags().entrySet()) {
      actualTags = Tags.concat(actualTags, e.getKey(), e.getValue());
    }
    rqueueMetricsProperties.setMetricTags(actualTags);
    RqueueCounter counter = new RqueueCounter();
    RqueueMetrics.monitor(
        rqueueMessageListenerContainer, meterRegistry, rqueueMetricsProperties, counter);
    return counter;
  }
}
