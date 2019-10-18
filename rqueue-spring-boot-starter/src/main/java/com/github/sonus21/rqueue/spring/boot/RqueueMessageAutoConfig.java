package com.github.sonus21.rqueue.spring.boot;

import com.github.sonus21.rqueue.config.SimpleRqueueListenerContainerFactory;
import com.github.sonus21.rqueue.core.RqueueMessageTemplate;
import com.github.sonus21.rqueue.listener.RqueueMessageHandler;
import com.github.sonus21.rqueue.listener.RqueueMessageListenerContainer;
import com.github.sonus21.rqueue.producer.RqueueMessageSender;
import org.springframework.beans.factory.BeanFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.redis.connection.RedisConnectionFactory;

@Configuration
public class RqueueMessageAutoConfig {

  @Autowired(required = false)
  private final SimpleRqueueListenerContainerFactory simpleRqueueListenerContainerFactory =
      new SimpleRqueueListenerContainerFactory();

  @Autowired private BeanFactory beanFactory;

  private RqueueMessageTemplate getMessageTemplate(RedisConnectionFactory connectionFactory) {
    if (simpleRqueueListenerContainerFactory.getRqueueMessageTemplate() != null) {
      return simpleRqueueListenerContainerFactory.getRqueueMessageTemplate();
    }
    simpleRqueueListenerContainerFactory.setRqueueMessageTemplate(
        new RqueueMessageTemplate(connectionFactory));
    return simpleRqueueListenerContainerFactory.getRqueueMessageTemplate();
  }

  private RedisConnectionFactory getRedisConnectionFactory() {
    if (simpleRqueueListenerContainerFactory.getRedisConnectionFactory() == null) {
      simpleRqueueListenerContainerFactory.setRedisConnectionFactory(
          beanFactory.getBean(RedisConnectionFactory.class));
    }
    return simpleRqueueListenerContainerFactory.getRedisConnectionFactory();
  }

  @Bean
  @ConditionalOnMissingBean
  public RqueueMessageHandler rqueueMessageHandler() {
    if (simpleRqueueListenerContainerFactory.getRqueueMessageHandler() != null) {
      return simpleRqueueListenerContainerFactory.getRqueueMessageHandler();
    }
    if (simpleRqueueListenerContainerFactory.getMessageConverters() != null) {
      return new RqueueMessageHandler(simpleRqueueListenerContainerFactory.getMessageConverters());
    }
    return new RqueueMessageHandler();
  }

  @Bean
  @ConditionalOnMissingBean
  public RqueueMessageListenerContainer simpleMessageListenerContainer(
      RqueueMessageHandler rqueueMessageHandler) {
    if (simpleRqueueListenerContainerFactory.getRqueueMessageHandler() == null) {
      simpleRqueueListenerContainerFactory.setRqueueMessageHandler(rqueueMessageHandler);
    }
    if (simpleRqueueListenerContainerFactory.getRedisConnectionFactory() == null) {
      simpleRqueueListenerContainerFactory.setRedisConnectionFactory(getRedisConnectionFactory());
    }
    return simpleRqueueListenerContainerFactory.createMessageListenerContainer();
  }

  @Bean
  @ConditionalOnMissingBean
  public RqueueMessageSender rqueueMessageSender() {
    if (simpleRqueueListenerContainerFactory.getMessageConverters() != null) {
      return new RqueueMessageSender(
          getMessageTemplate(getRedisConnectionFactory()),
          simpleRqueueListenerContainerFactory.getMessageConverters());
    }
    return new RqueueMessageSender(getMessageTemplate(getRedisConnectionFactory()));
  }
}
