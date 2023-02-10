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

package com.github.sonus21.rqueue.config;

import static org.springframework.util.Assert.notEmpty;
import static org.springframework.util.Assert.notNull;

import com.github.sonus21.rqueue.annotation.MessageListener;
import com.github.sonus21.rqueue.annotation.RqueueListener;
import com.github.sonus21.rqueue.converter.DefaultMessageConverterProvider;
import com.github.sonus21.rqueue.converter.MessageConverterProvider;
import com.github.sonus21.rqueue.core.RqueueMessageTemplate;
import com.github.sonus21.rqueue.core.impl.RqueueMessageTemplateImpl;
import com.github.sonus21.rqueue.core.middleware.Middleware;
import com.github.sonus21.rqueue.core.support.MessageProcessor;
import com.github.sonus21.rqueue.listener.RqueueMessageHandler;
import com.github.sonus21.rqueue.listener.RqueueMessageListenerContainer;
import com.github.sonus21.rqueue.models.enums.PriorityMode;
import com.github.sonus21.rqueue.utils.Constants;
import com.github.sonus21.rqueue.utils.backoff.TaskExecutionBackOff;
import java.util.LinkedList;
import java.util.List;
import org.springframework.core.task.AsyncTaskExecutor;
import org.springframework.core.task.TaskExecutor;
import org.springframework.data.redis.connection.ReactiveRedisConnectionFactory;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.messaging.MessageHeaders;
import org.springframework.messaging.converter.MessageConverter;
import org.springframework.util.CollectionUtils;

/**
 * A Factory class which can be used to create {@link RqueueMessageListenerContainer} object.
 * Instead of going through lower level detail of {@link RqueueMessageListenerContainer}.
 *
 * <p>Factory has multiple methods to support different types of requirements.
 */
@SuppressWarnings("WeakerAccess")
public class SimpleRqueueListenerContainerFactory {

  private final List<Middleware> middlewares = new LinkedList<>();
  // The message converter provider that will return a message converter to convert messages to/from
  private MessageConverterProvider messageConverterProvider;
  // Provide task executor, this can be used to provide some additional details like some threads
  // name, etc otherwise a default task executor would be created
  private AsyncTaskExecutor taskExecutor;
  // whether container should auto start or not
  private boolean autoStartup = true;
  // Redis connection factory for the listener container
  private RedisConnectionFactory redisConnectionFactory;
  // Reactive redis connection factory for the listener
  private ReactiveRedisConnectionFactory reactiveRedisConnectionFactory;
  // Custom requeue message handler
  private RqueueMessageHandler rqueueMessageHandler;
  // polling interval (millisecond) when no messages are available
  private long pollingInterval = 200L;
  // In case of job execution failure how long(in millisecond) this job should be delayed
  private long backOffTime = 5 * Constants.ONE_MILLI;
  // Number of workers requires for listeners
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
  // Any message headers that should be set, headers are NOT STORED in db so it should not be
  // changed, same header is used in serialized and deserialization process.
  private MessageHeaders messageHeaders;

  // Set priority mode for the pollers
  private PriorityMode priorityMode = PriorityMode.WEIGHTED;

  /**
   * Whether all beans of spring application should be inspected to find methods annotated with
   * {@link RqueueListener}, otherwise it will only inspect beans those are annotated with
   * {@link MessageListener}
   */
  private boolean inspectAllBean = true;

  /**
   * Whenever a consumer fails then the consumed message can be scheduled for further retry. The
   * delay of such retry can be configured based on the different configuration, by default same
   * message would be retried in 5 seconds and this will continue till all retries are not exhausted
   * due to default task interval.
   * {@link com.github.sonus21.rqueue.utils.backoff.FixedTaskExecutionBackOff#DEFAULT_INTERVAL}
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
   * the handler methods. If no {@link TaskExecutor} is set, a default one is created. If you're
   * setting this then you should set {@link #maxNumWorkers}.
   *
   * @param taskExecutor The {@link TaskExecutor} used by the container.
   * @see RqueueMessageListenerContainer#createDefaultTaskExecutor(List)
   * @see #setMaxNumWorkers(int)
   */
  public void setTaskExecutor(AsyncTaskExecutor taskExecutor) {
    notNull(taskExecutor, "taskExecutor cannot be null");
    this.taskExecutor = taskExecutor;
  }

  public Boolean getAutoStartup() {
    return autoStartup;
  }

  /**
   * Configures if {@link RqueueMessageListenerContainer} container should be automatically started.
   * The default value is true.
   *
   * <p>Setting this to false means, you must call start, stop and destroy methods of {@link
   * RqueueMessageListenerContainer#start()},
   * {@link RqueueMessageListenerContainer#stop() },{@link
   * RqueueMessageListenerContainer#destroy()}
   *
   * @param autoStartup - false if the container will be manually started
   */
  public void setAutoStartup(boolean autoStartup) {
    this.autoStartup = autoStartup;
  }

  /**
   * Return configured message handler
   *
   * @param messageConverterProvider message converter that will be used to serialize/deserialize
   *                                 message
   * @return RqueueMessageHandler object
   */
  public RqueueMessageHandler getRqueueMessageHandler(
      MessageConverterProvider messageConverterProvider) {
    if (this.messageConverterProvider == null) {
      this.messageConverterProvider = messageConverterProvider;
    }
    notNull(this.messageConverterProvider, "messageConverterProvider can not be null");
    if (rqueueMessageHandler == null) {
      rqueueMessageHandler =
          new RqueueMessageHandler(messageConverterProvider.getConverter(), inspectAllBean);
    }
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
   * an error occurs (e.g. connection timeout)
   */
  public long getBackOffTime() {
    return backOffTime;
  }

  /**
   * The number of milliseconds the polling thread must wait before trying to recover when an error
   * occurs (e.g. connection timeout). Default value is 10000 milliseconds.
   *
   * @param backOffTime in milliseconds.
   */
  public void setBackOffTime(long backOffTime) {
    this.backOffTime = backOffTime;
  }

  public Integer getMaxNumWorkers() {
    return maxNumWorkers;
  }

  /**
   * Maximum number of workers, that would be used to run tasks. The default size which is 2 threads
   * for every queue.
   *
   * <p>When you're using custom executor then you should set this number as (thread pool max size
   * - number of queues) given executor is not shared with other application component. The
   * maxNumWorkers tells how many workers you want to run in parallel for all listeners those are
   * not having configured concurrency. For example if you have 3 queues without concurrency, and
   * you have set this as 10 then all 3 listeners would be running maximum **combined 10 jobs** at
   * any point of time. Queues having concurrency will be running at the configured concurrency.
   *
   * <p>What would happen if I set this to very high value while using custom executor? <br>
   * 1. Task(s) would be rejected by the executor unless queue size is non-zero <br> 2. When queue
   * size is non-zero then it can create duplicate message problem, since the polled message has not
   * been processed yet. This will happen when {@link RqueueListener#visibilityTimeout()} is smaller
   * than the time a task takes to execute from the time of polling to final execution.
   *
   * @param maxNumWorkers Maximum number of workers.
   */
  public void setMaxNumWorkers(int maxNumWorkers) {
    if (maxNumWorkers < 1) {
      throw new IllegalArgumentException("At least one worker");
    }
    this.maxNumWorkers = maxNumWorkers;
  }

  /**
   * Message converter must be configured before calling this method.
   *
   * @return the message converter object
   * @throws IllegalAccessException when messageConverterProvider is null
   */
  public MessageConverter getMessageConverter() throws IllegalAccessException {
    if (messageConverterProvider == null) {
      throw new IllegalAccessException("messageConverterProvider is not configured");
    }
    return messageConverterProvider.getConverter();
  }

  /**
   * Rqueue configures a default message converter that can convert message to/from for any object.
   *
   * @param messageConverterProvider the message converter provider
   * @see DefaultMessageConverterProvider
   */
  public void setMessageConverterProvider(MessageConverterProvider messageConverterProvider) {
    notNull(messageConverterProvider, "messageConverterProvider must not be null");
    this.messageConverterProvider = messageConverterProvider;
  }

  /**
   * @return get Redis connection factor
   */
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

  /**
   * @return message template
   */
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
   * need redis connection factory {@link RedisConnectionFactory }as well as message handler
   * {@link RqueueMessageHandler}.
   *
   * @return an object of {@link RqueueMessageListenerContainer} object
   */
  public RqueueMessageListenerContainer createMessageListenerContainer() {
    notNull(redisConnectionFactory, "redisConnectionFactory must not be null");
    notNull(messageConverterProvider, "messageConverterProvider must not be null");
    if (rqueueMessageTemplate == null) {
      rqueueMessageTemplate =
          new RqueueMessageTemplateImpl(
              getRedisConnectionFactory(), getReactiveRedisConnectionFactory());
    }
    RqueueMessageListenerContainer messageListenerContainer =
        new RqueueMessageListenerContainer(
            getRqueueMessageHandler(messageConverterProvider), rqueueMessageTemplate);
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
    if (!CollectionUtils.isEmpty(getMiddlewares())) {
      messageListenerContainer.setMiddlewares(getMiddlewares());
    }
    if (messageHeaders != null) {
      messageListenerContainer.setMessageHeaders(messageHeaders);
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
   * @return the time in milliseconds
   */
  public long getPollingInterval() {
    return pollingInterval;
  }

  /**
   * Set polling time interval, this controls the listener polling interval
   *
   * @param pollingInterval time in milliseconds
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

  /**
   * Set priority mode for queues.
   *
   * @param priorityMode strict or weighted.
   */
  public void setPriorityMode(PriorityMode priorityMode) {
    this.priorityMode = priorityMode;
  }

  public MessageHeaders getMessageHeaders() {
    return messageHeaders;
  }

  public void setMessageHeaders(MessageHeaders messageHeaders) {
    notEmpty(messageHeaders, "messageHeaders can not be empty");
    this.messageHeaders = messageHeaders;
  }

  /**
   * Rqueue scans all beans to find method annotated with {@link RqueueListener}.
   *
   * <p>Scanning all beans can slow the bootstrap process, by default this is enabled, but using
   * {@link MessageListener} the scanned beans can be limited.
   *
   * <p>If you just want to scan specific beans for {@link RqueueListener} annotated methods, than
   * set {@link #inspectAllBean} to false and annotate your message listener classes with
   * {@link MessageListener}
   *
   * @param inspectAllBean whether all beans should be inspected for {@link RqueueListener} or not
   * @see MessageListener
   */
  public void setInspectAllBean(boolean inspectAllBean) {
    this.inspectAllBean = inspectAllBean;
  }

  /**
   * Get configured middlewares
   *
   * @return list of middlewares or null
   */
  public List<Middleware> getMiddlewares() {
    return middlewares;
  }

  /**
   * Add middlewares those would be used while processing a message. Middlewares are called in the
   * order they are added.
   *
   * @param middlewares list of middlewares
   */
  public void setMiddlewares(List<Middleware> middlewares) {
    notEmpty(middlewares, "middlewares cannot be empty");
    this.middlewares.addAll(middlewares);
  }

  /**
   * Add a given middleware in the chain
   *
   * @param middleware middleware
   */
  public void useMiddleware(Middleware middleware) {
    notNull(middlewares, "middlewares cannot be null");
    this.middlewares.add(middleware);
  }

  public ReactiveRedisConnectionFactory getReactiveRedisConnectionFactory() {
    return reactiveRedisConnectionFactory;
  }

  /**
   * Set Reactive redis connection factory
   *
   * @param reactiveRedisConnectionFactory reactive redis connection factory to be used
   */
  public void setReactiveRedisConnectionFactory(
      ReactiveRedisConnectionFactory reactiveRedisConnectionFactory) {
    notNull(reactiveRedisConnectionFactory, "reactiveRedisConnectionFactory can not be null");
    this.reactiveRedisConnectionFactory = reactiveRedisConnectionFactory;
  }
}
