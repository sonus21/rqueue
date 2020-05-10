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

package com.github.sonus21.rqueue.config;

import static org.springframework.util.Assert.notEmpty;
import static org.springframework.util.Assert.notNull;

import com.github.sonus21.rqueue.core.RqueueMessageTemplate;
import com.github.sonus21.rqueue.core.RqueueMessageTemplateImpl;
import com.github.sonus21.rqueue.core.support.MessageProcessor;
import com.github.sonus21.rqueue.listener.RqueueMessageHandler;
import com.github.sonus21.rqueue.listener.RqueueMessageListenerContainer;
import com.github.sonus21.rqueue.models.enums.PriorityMode;
import com.github.sonus21.rqueue.utils.Constants;
import com.github.sonus21.rqueue.utils.backoff.TaskExecutionBackOff;
import java.util.List;
import org.springframework.core.task.AsyncTaskExecutor;
import org.springframework.core.task.TaskExecutor;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.messaging.converter.MessageConverter;

/**
 * A Factory class which can be used to create {@link RqueueMessageListenerContainer} object.
 * Instead of going through lower level detail of {@link RqueueMessageListenerContainer}.
 *
 * <p>Factory has multiple methods to support different types of requirements.
 */
public class SimpleRqueueListenerContainerFactory {
  // Provide task executor, this can be used to provide some additional details like some threads
  // name, etc otherwise a default task executor would be created
  private AsyncTaskExecutor taskExecutor;
  // whether container should auto start or not
  private boolean autoStartup = true;
  // Redis connection factory for the listener container
  private RedisConnectionFactory redisConnectionFactory;
  // Custom requeue message handler
  private RqueueMessageHandler rqueueMessageHandler;
  // List of message converters to convert messages to/from
  private List<MessageConverter> messageConverters;
  // Send message poll time when no messages are available
  private long pollingInterval = 200L;
  // In case of failure how much time, we should wait for next job
  private long backOffTime = 5 * Constants.ONE_MILLI;
  // Number of workers requires for execution
  private Integer maxNumWorkers;

  // This message processor would be called before a task can start execution.
  // It needs to be noted that this message processor would be called multiple time
  // In case of retry, so application should be able to handle that.
  private MessageProcessor preExecutionMessageProcessor;
  // This message processor would be called whenever a message is discarded due to retry limit
  // exhaustion
  private MessageProcessor discardMessageProcessor;
  // This message processor would be called whenever a message is moved to dead letter queue
  private MessageProcessor deadLetterQueueMessageProcessor;
  // This message processor would be called whenever a message is delete manually
  private MessageProcessor manualDeletionMessageProcessor;
  // This message processor would be called whenever a message executed successfully.
  private MessageProcessor postExecutionMessageProcessor;
  // Any custom message requeue message template.
  private RqueueMessageTemplate rqueueMessageTemplate;

  // Set priority mode for the workers
  private PriorityMode priorityMode;

  /**
   * Whenever a consumer fails then the consumed message can be delayed for further consumption. The
   * delay of that can be configured, by default same message would be retried in 5 seconds and this
   * will continue due to default task interval. {@link
   * com.github.sonus21.rqueue.utils.backoff.FixedTaskExecutionBackOff#DEFAULT_INTERVAL}
   *
   * @see com.github.sonus21.rqueue.utils.backoff.ExponentialTaskExecutionBackOff
   * @see com.github.sonus21.rqueue.utils.backoff.FixedTaskExecutionBackOff
   */
  private TaskExecutionBackOff taskExecutionBackOff;

  /**
   * Get configured task executor
   *
   * @return async task executor
   */
  public AsyncTaskExecutor getTaskExecutor() {
    return taskExecutor;
  }

  /**
   * Configures the {@link TaskExecutor} which is used to poll messages and execute them by calling
   * the handler methods. If no {@link TaskExecutor} is set, a default one is created.
   *
   * @param taskExecutor The {@link TaskExecutor} used by the container.
   * @see RqueueMessageListenerContainer#createDefaultTaskExecutor()
   */
  public void setTaskExecutor(AsyncTaskExecutor taskExecutor) {
    notNull(taskExecutor, "taskExecutor can not be null");
    this.taskExecutor = taskExecutor;
  }

  public Boolean getAutoStartup() {
    return autoStartup;
  }

  /**
   * Configures if {@link com.github.sonus21.rqueue.listener.RqueueMessageListenerContainer}
   * container should be automatically started. The default value is true.
   *
   * @param autoStartup - false if the container will be manually started
   */
  public void setAutoStartup(boolean autoStartup) {
    this.autoStartup = autoStartup;
  }

  /**
   * Return configured message handler
   *
   * @return RqueueMessageHandler object
   */
  public RqueueMessageHandler getRqueueMessageHandler() {
    return rqueueMessageHandler;
  }

  /**
   * Set message handler, this can be used to set custom message handlers that could have some
   * special features apart of the default one
   *
   * @param rqueueMessageHandler {@link RqueueMessageHandler} object
   */
  public void setRqueueMessageHandler(RqueueMessageHandler rqueueMessageHandler) {
    notNull(rqueueMessageHandler, "rqueueMessageHandler must not be null");
    this.rqueueMessageHandler = rqueueMessageHandler;
  }

  /**
   * @return The number of milliseconds the polling thread must wait before trying to recover when
   *     an error occurs (e.g. connection timeout)
   */
  public long getBackOffTime() {
    return backOffTime;
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

  public Integer getMaxNumWorkers() {
    return maxNumWorkers;
  }

  /**
   * Maximum number of workers, that would be used to run tasks.
   *
   * @param maxNumWorkers Maximum number of workers.
   */
  public void setMaxNumWorkers(int maxNumWorkers) {
    if (maxNumWorkers < 1) {
      throw new IllegalArgumentException("At least one worker");
    }
    this.maxNumWorkers = maxNumWorkers;
  }

  /** @return list of configured message converters */
  public List<MessageConverter> getMessageConverters() {
    return messageConverters;
  }

  /**
   * For message (de)serialization we might need one or more message converters, configure those
   * message converters
   *
   * @param messageConverters list of message converters
   */
  public void setMessageConverters(List<MessageConverter> messageConverters) {
    notEmpty(messageConverters, "messageConverters must not be empty");
    this.messageConverters = messageConverters;
  }

  /** @return get Redis connection factor */
  public RedisConnectionFactory getRedisConnectionFactory() {
    return redisConnectionFactory;
  }

  /**
   * Set redis connection factory, that would be used to configured message template and other
   * components
   *
   * @param redisConnectionFactory redis connection factory object
   */
  public void setRedisConnectionFactory(RedisConnectionFactory redisConnectionFactory) {
    notNull(redisConnectionFactory, "redisConnectionFactory must not be null");
    this.redisConnectionFactory = redisConnectionFactory;
  }

  /** @return message template */
  public RqueueMessageTemplate getRqueueMessageTemplate() {
    return rqueueMessageTemplate;
  }

  /**
   * Set RqueueMessageTemplate that's used to pull and push messages from/to Redis.
   *
   * @param messageTemplate a message template object.
   */
  public void setRqueueMessageTemplate(RqueueMessageTemplate messageTemplate) {
    notNull(messageTemplate, "messageTemplate must not be null");
    rqueueMessageTemplate = messageTemplate;
  }

  /**
   * Creates a {@link RqueueMessageListenerContainer} container. To create this container we would
   * need redis connection factory {@link RedisConnectionFactory }as well as message handler {@link
   * RqueueMessageHandler}.
   *
   * @return an object of {@link RqueueMessageListenerContainer} object
   */
  public RqueueMessageListenerContainer createMessageListenerContainer() {
    notNull(rqueueMessageHandler, "rqueueMessageHandler must not be null");
    notNull(redisConnectionFactory, "redisConnectionFactory must not be null");
    if (rqueueMessageTemplate == null) {
      rqueueMessageTemplate = new RqueueMessageTemplateImpl(redisConnectionFactory);
    }
    RqueueMessageListenerContainer messageListenerContainer =
        new RqueueMessageListenerContainer(rqueueMessageHandler, rqueueMessageTemplate);
    messageListenerContainer.setAutoStartup(autoStartup);
    if (taskExecutor != null) {
      messageListenerContainer.setTaskExecutor(taskExecutor);
    }
    if (maxNumWorkers != null) {
      messageListenerContainer.setMaxNumWorkers(maxNumWorkers);
    }
    messageListenerContainer.setBackOffTime(getBackOffTime());
    messageListenerContainer.setPollingInterval(getPollingInterval());
    if (postExecutionMessageProcessor != null) {
      messageListenerContainer.setPostExecutionMessageProcessor(getPostExecutionMessageProcessor());
    }
    if (deadLetterQueueMessageProcessor != null) {
      messageListenerContainer.setDeadLetterQueueMessageProcessor(
          getDeadLetterQueueMessageProcessor());
    }
    if (manualDeletionMessageProcessor != null) {
      messageListenerContainer.setManualDeletionMessageProcessor(
          getManualDeletionMessageProcessor());
    }
    if (discardMessageProcessor != null) {
      messageListenerContainer.setDiscardMessageProcessor(discardMessageProcessor);
    }
    if (getPreExecutionMessageProcessor() != null) {
      messageListenerContainer.setPreExecutionMessageProcessor(preExecutionMessageProcessor);
    }
    if (getTaskExecutionBackOff() != null) {
      messageListenerContainer.setTaskExecutionBackOff(getTaskExecutionBackOff());
    }
    if (getPriorityMode() != null) {
      messageListenerContainer.setPriorityMode(getPriorityMode());
    }
    return messageListenerContainer;
  }

  public MessageProcessor getDiscardMessageProcessor() {
    return discardMessageProcessor;
  }

  /**
   * This message processor would be called whenever a message is discarded due to retry limit
   * exhaust.
   *
   * @param discardMessageProcessor object of the discard message processor.
   */
  public void setDiscardMessageProcessor(MessageProcessor discardMessageProcessor) {
    notNull(discardMessageProcessor, "discardMessageProcessor cannot be null");
    this.discardMessageProcessor = discardMessageProcessor;
  }

  public MessageProcessor getDeadLetterQueueMessageProcessor() {
    return deadLetterQueueMessageProcessor;
  }

  /**
   * This message processor would be called whenever a message is moved to dead letter queue
   *
   * @param deadLetterQueueMessageProcessor object of message processor.
   */
  public void setDeadLetterQueueMessageProcessor(MessageProcessor deadLetterQueueMessageProcessor) {
    notNull(deadLetterQueueMessageProcessor, "deadLetterQueueMessageProcessor cannot be null");
    this.deadLetterQueueMessageProcessor = deadLetterQueueMessageProcessor;
  }

  public MessageProcessor getManualDeletionMessageProcessor() {
    return manualDeletionMessageProcessor;
  }

  /**
   * Set a message processor would be called whenever a manual deletion message is encounter during
   * execution.
   *
   * @param manualDeletionMessageProcessor object of message processor.
   */
  public void setManualDeletionMessageProcessor(MessageProcessor manualDeletionMessageProcessor) {
    notNull(manualDeletionMessageProcessor, "manualDeletionMessageProcessor cannot be null");
    this.manualDeletionMessageProcessor = manualDeletionMessageProcessor;
  }

  public MessageProcessor getPostExecutionMessageProcessor() {
    return postExecutionMessageProcessor;
  }

  /**
   * A message processor would be called whenever a consumer has successfully consumed the given
   * message.
   *
   * @param postExecutionMessageProcessor object of message processor.
   */
  public void setPostExecutionMessageProcessor(MessageProcessor postExecutionMessageProcessor) {
    notNull(postExecutionMessageProcessor, "postExecutionMessageProcessor cannot be null");
    this.postExecutionMessageProcessor = postExecutionMessageProcessor;
  }

  public MessageProcessor getPreExecutionMessageProcessor() {
    return preExecutionMessageProcessor;
  }

  /**
   * A message processor that can control the execution of any task, this message processor would be
   * called multiple time, in case of retry, so application should be able to handle this. If this
   * message processor returns false then message won't be executed, it will be considered that task
   * has to be deleted.
   *
   * @param preExecutionMessageProcessor pre execution message processor.
   */
  public void setPreExecutionMessageProcessor(MessageProcessor preExecutionMessageProcessor) {
    notNull(preExecutionMessageProcessor, "preMessageProcessor cannot be null");
    this.preExecutionMessageProcessor = preExecutionMessageProcessor;
  }

  /**
   * Get configured polling interval
   *
   * @return the time in milli seconds
   */
  public long getPollingInterval() {
    return pollingInterval;
  }

  /**
   * Set polling time interval, this controls the listener polling interval
   *
   * @param pollingInterval time in milli seconds
   */
  public void setPollingInterval(long pollingInterval) {
    this.pollingInterval = pollingInterval;
  }

  /**
   * Return the task execution back-off
   *
   * @return the back-off provider
   */
  public TaskExecutionBackOff getTaskExecutionBackOff() {
    return taskExecutionBackOff;
  }

  /**
   * Set custom task executor back-off.
   *
   * @param taskExecutionBackOff task execution back-off.
   */
  public void setTaskExecutionBackOff(TaskExecutionBackOff taskExecutionBackOff) {
    notNull(taskExecutionBackOff, "taskExecutionBackOff cannot be null");
    this.taskExecutionBackOff = taskExecutionBackOff;
  }

  public PriorityMode getPriorityMode() {
    return priorityMode;
  }

  public void setPriorityMode(PriorityMode priorityMode) {
    this.priorityMode = priorityMode;
  }
}
