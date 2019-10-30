/*
 * Copyright (c)  2019-2019, Sonu Kumar
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *       https://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */

package com.github.sonus21.rqueue.spring.boot.integration;

import static com.github.sonus21.rqueue.utils.QueueInfo.getProcessingQueueChannelName;
import static com.github.sonus21.rqueue.utils.QueueInfo.getProcessingQueueName;

import com.github.sonus21.rqueue.config.SimpleRqueueListenerContainerFactory;
import com.github.sonus21.rqueue.core.RqueueMessage;
import com.github.sonus21.rqueue.core.RqueueMessageTemplate;
import com.github.sonus21.rqueue.listener.RqueueMessageHandler;
import com.github.sonus21.rqueue.listener.RqueueMessageListenerContainer;
import java.util.Arrays;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.PropertySource;
import org.springframework.data.jpa.repository.config.EnableJpaRepositories;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.repository.configuration.EnableRedisRepositories;
import org.springframework.transaction.annotation.EnableTransactionManagement;

@PropertySource("classpath:application-disabled.properties")
@SpringBootApplication(scanBasePackages = {"com.github.sonus21.rqueue.spring.boot.integration.app"})
@EnableRedisRepositories
@EnableJpaRepositories(
    basePackages = {"com.github.sonus21.rqueue.spring.boot.integration.app.repository"})
@EnableTransactionManagement
public class ApplicationWithAutoStartSchedulerDisabled extends BaseApplication {

  @Bean
  public RqueueMessageTemplate rqueueMessageTemplate(
      RedisConnectionFactory redisConnectionFactory) {
    // set visibility timeout to 10 seconds
    return new RqueueMessageTemplate(redisConnectionFactory) {
      @Override
      public RqueueMessage pop(String queueName) {
        long currentTime = System.currentTimeMillis();
        return scriptExecutor.execute(
            removeScript,
            Arrays.asList(
                queueName,
                getProcessingQueueName(queueName),
                getProcessingQueueChannelName(queueName)),
            currentTime,
            currentTime + 10000L);
      }
    };
  }

  @Bean
  public RqueueMessageListenerContainer rqueueMessageListenerContainer(
      RedisConnectionFactory redisConnectionFactory,
      RqueueMessageHandler rqueueMessageHandler,
      RqueueMessageTemplate rqueueMessageTemplate) {
    SimpleRqueueListenerContainerFactory factory = new SimpleRqueueListenerContainerFactory();
    factory.setRqueueMessageHandler(rqueueMessageHandler);
    factory.setRedisConnectionFactory(redisConnectionFactory);
    factory.setRqueueMessageTemplate(rqueueMessageTemplate);
    RqueueMessageListenerContainer container = factory.createMessageListenerContainer();
    container.setMaxNumWorkers(100);
    return container;
  }

  public static void main(String[] args) {
    SpringApplication.run(ApplicationWithAutoStartSchedulerDisabled.class, args);
  }
}
