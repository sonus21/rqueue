package io.rqueue.config;

import io.rqueue.core.RqueueMessageTemplate;
import io.rqueue.core.StringMessageTemplate;
import io.rqueue.listener.RqueueMessageHandler;
import io.rqueue.listener.RqueueMessageListenerContainer;
import java.util.List;
import org.springframework.core.task.AsyncTaskExecutor;
import org.springframework.core.task.TaskExecutor;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.messaging.converter.MessageConverter;
import org.springframework.util.Assert;

public class SimpleRqueueListenerContainerFactory {
  private AsyncTaskExecutor taskExecutor;
  private boolean autoStartup = true;
  private RqueueMessageTemplate rqueueMessageTemplate;
  private RedisConnectionFactory redisConnectionFactory;
  private RqueueMessageHandler rqueueMessageHandler;
  private StringMessageTemplate stringMessageTemplate;
  private List<MessageConverter> messageConverters;
  private Long backOffTime;
  private Integer maxNumWorkers;

  /**
   * Configures the {@link TaskExecutor} which is used to poll messages and execute them by calling
   * the handler methods. If no {@link TaskExecutor} is set, a default one is created.
   *
   * @param taskExecutor The {@link TaskExecutor} used by the container
   * @see RqueueMessageListenerContainer#createDefaultTaskExecutor()
   */
  public void setTaskExecutor(AsyncTaskExecutor taskExecutor) {
    this.taskExecutor = taskExecutor;
  }

  /**
   * Configures if this container should be automatically started. The default value is true.
   *
   * @param autoStartup - false if the container will be manually started
   */
  public void setAutoStartup(boolean autoStartup) {
    this.autoStartup = autoStartup;
  }

  public RqueueMessageHandler getRqueueMessageHandler() {
    return this.rqueueMessageHandler;
  }

  public void setRqueueMessageHandler(RqueueMessageHandler rqueueMessageHandler) {
    Assert.notNull(rqueueMessageHandler, "rqueueMessageHandler must not be null");
    this.rqueueMessageHandler = rqueueMessageHandler;
  }

  /**
   * @return The number of milliseconds the polling thread must wait before trying to recover when
   *     an error occurs (e.g. connection timeout)
   */
  public Long getBackOffTime() {
    return this.backOffTime;
  }

  /**
   * The number of milliseconds the polling thread must wait before trying to recover when an error
   * occurs (e.g. connection timeout). Default value is 10000 milliseconds.
   *
   * @param backOffTime in milliseconds
   */
  public void setBackOffTime(long backOffTime) {
    this.backOffTime = backOffTime;
  }

  public void setMaxNumWorkers(int maxNumWorkers) {
    this.maxNumWorkers = maxNumWorkers;
  }

  public List<MessageConverter> getMessageConverters() {
    return messageConverters;
  }

  public void setMessageConverters(List<MessageConverter> messageConverters) {
    Assert.notNull(messageConverters, "messageConverters must not be null");
    this.messageConverters = messageConverters;
  }

  public void setRedisConnectionFactory(RedisConnectionFactory redisConnectionFactory) {
    Assert.notNull(redisConnectionFactory, "redisConnectionFactory must not be null");
    this.redisConnectionFactory = redisConnectionFactory;
  }

  public RedisConnectionFactory getRedisConnectionFactory() {
    return this.redisConnectionFactory;
  }

  public RqueueMessageTemplate getRqueueMessageTemplate() {
    return this.rqueueMessageTemplate;
  }

  public void setRqueueMessageTemplate(RqueueMessageTemplate messageTemplate) {
    Assert.notNull(messageTemplate, "messageTemplate must not be null");
    this.rqueueMessageTemplate = messageTemplate;
  }

  public RqueueMessageListenerContainer createMessageListenerContainer() {
    Assert.notNull(this.rqueueMessageHandler, "rqueueMessageHandler must not be null");
    Assert.notNull(this.redisConnectionFactory, "redisConnectionFactory must not be null");
    if (this.rqueueMessageTemplate == null) {
      this.rqueueMessageTemplate = new RqueueMessageTemplate(redisConnectionFactory);
    }
    if (this.stringMessageTemplate == null) {
      this.stringMessageTemplate = new StringMessageTemplate(this.redisConnectionFactory);
    }
    RqueueMessageListenerContainer messageListenerContainer =
        new RqueueMessageListenerContainer(
            this.rqueueMessageTemplate, this.rqueueMessageHandler, this.stringMessageTemplate);
    messageListenerContainer.setAutoStartup(this.autoStartup);
    if (this.taskExecutor != null) {
      messageListenerContainer.setTaskExecutor(this.taskExecutor);
    }
    if (this.maxNumWorkers != null) {
      messageListenerContainer.setMaxNumWorkers(this.maxNumWorkers);
    }
    if (this.backOffTime != null) {
      messageListenerContainer.setBackOffTime(this.backOffTime);
    }
    return messageListenerContainer;
  }
}
