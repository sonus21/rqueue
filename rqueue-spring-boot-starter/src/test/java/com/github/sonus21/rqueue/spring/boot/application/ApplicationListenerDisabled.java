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

package com.github.sonus21.rqueue.spring.boot.application;

import com.github.sonus21.rqueue.core.RqueueMessageTemplate;
import com.github.sonus21.rqueue.core.impl.RqueueMessageTemplateImpl;
import com.github.sonus21.rqueue.listener.QueueDetail;
import com.github.sonus21.rqueue.listener.RqueueMessageHandler;
import com.github.sonus21.rqueue.listener.RqueueMessageListenerContainer;
import com.github.sonus21.rqueue.test.application.BaseApplication;
import javax.annotation.PostConstruct;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.PropertySource;
import org.springframework.data.jpa.repository.config.EnableJpaRepositories;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.repository.configuration.EnableRedisRepositories;
import org.springframework.test.context.TestPropertySource;
import org.springframework.transaction.annotation.EnableTransactionManagement;

@PropertySource("classpath:application.properties")
@SpringBootApplication(scanBasePackages = {"com.github.sonus21.rqueue.test"})
@EnableRedisRepositories
@EnableJpaRepositories(basePackages = {"com.github.sonus21.rqueue.test.repository"})
@EnableTransactionManagement
public class ApplicationListenerDisabled extends BaseApplication {
  public static void main(String[] args) {
    SpringApplication.run(ApplicationListenerDisabled.class, args);
  }

  @PostConstruct
  @Override
  public void postConstruct() {
    init();
    monitor(redisHost, redisPort);
  }

  @Bean
  public RqueueMessageTemplate rqueueMessageTemplate(
      RedisConnectionFactory redisConnectionFactory) {
    return new RqueueMessageTemplateImpl(redisConnectionFactory);
  }

  @Bean
  public RqueueMessageListenerContainer rqueueMessageListenerContainer(
      RqueueMessageHandler rqueueMessageHandler, RqueueMessageTemplate rqueueMessageTemplate) {
    return new RqueueMessageListenerContainer(rqueueMessageHandler, rqueueMessageTemplate) {
      @Override
      protected void startQueue(String queueName, QueueDetail queueDetail) {}
    };
  }
}
