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

package com.github.sonus21.rqueue.listener;

import static com.github.sonus21.rqueue.utils.Constants.DEFAULT_WORKER_COUNT_PER_QUEUE;
import static org.springframework.util.Assert.notNull;

import com.github.sonus21.rqueue.config.RqueueWebConfig;
import com.github.sonus21.rqueue.core.RqueueMessageTemplate;
import com.github.sonus21.rqueue.core.support.MessageProcessor;
import com.github.sonus21.rqueue.metrics.RqueueCounter;
import com.github.sonus21.rqueue.models.ThreadCount;
import com.github.sonus21.rqueue.models.event.QueueInitializationEvent;
import com.github.sonus21.rqueue.utils.Constants;
import com.github.sonus21.rqueue.utils.ThreadUtils;
import com.github.sonus21.rqueue.web.service.RqueueMessageMetadataService;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Future;
import java.util.concurrent.Semaphore;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.BeanNameAware;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.context.Lifecycle;
import org.springframework.context.SmartLifecycle;
import org.springframework.core.task.AsyncTaskExecutor;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.util.Assert;
import org.springframework.util.ClassUtils;

/**
 * Container providing asynchronous behaviour for Rqueue message listeners. Handles the low level
 * details of listening, converting and message dispatching.
 *
 * @see com.github.sonus21.rqueue.config.SimpleRqueueListenerContainerFactory
 */
@Slf4j
public class RqueueMessageListenerContainer
    implements InitializingBean, DisposableBean, SmartLifecycle, BeanNameAware {
  private static final String DEFAULT_THREAD_NAME_PREFIX =
      ClassUtils.getShortName(RqueueMessageListenerContainer.class);
  private final Object lifecycleMgr = new Object();
  private final RqueueMessageTemplate rqueueMessageTemplate;
  private final RqueueMessageHandler rqueueMessageHandler;
  private MessageProcessor discardMessageProcessor;
  private MessageProcessor deadLetterQueueMessageProcessor;
  private MessageProcessor manualDeletionMessageProcessor;
  private MessageProcessor postExecutionMessageProcessor;
  private MessageProcessor preExecutionMessageProcessor;

  @Autowired private ApplicationEventPublisher applicationEventPublisher;
  @Autowired private RqueueWebConfig rqueueWebConfig;

  @Autowired(required = false)
  private RqueueCounter rqueueCounter;

  @Autowired private RqueueMessageMetadataService rqueueMessageMetadataService;

  private Integer maxNumWorkers;
  private String beanName;
  private boolean defaultTaskExecutor = false;
  private AsyncTaskExecutor taskExecutor;
  private AsyncTaskExecutor spinningTaskExecutor;
  private boolean autoStartup = true;
  private Map<String, QueueDetail> registeredQueues = new ConcurrentHashMap<>();
  private Map<String, Boolean> queueRunningState = new ConcurrentHashMap<>();
  private ConcurrentHashMap<String, Future<?>> scheduledFutureByQueue = new ConcurrentHashMap<>();
  private boolean running = false;
  private long backOffTime = 5 * Constants.ONE_MILLI;
  private long maxWorkerWaitTime = 20 * Constants.ONE_MILLI;
  private long pollingInterval = 200L;
  private Semaphore semaphore;
  private int phase = Integer.MAX_VALUE;

  public RqueueMessageListenerContainer(
      RqueueMessageHandler rqueueMessageHandler, RqueueMessageTemplate rqueueMessageTemplate) {
    notNull(rqueueMessageHandler, "rqueueMessageHandler cannot be null");
    notNull(rqueueMessageTemplate, "rqueueMessageTemplate cannot be null");
    this.rqueueMessageHandler = rqueueMessageHandler;
    this.rqueueMessageTemplate = rqueueMessageTemplate;
    this.discardMessageProcessor = new MessageProcessor() {};
    this.deadLetterQueueMessageProcessor = this.discardMessageProcessor;
    this.manualDeletionMessageProcessor = this.discardMessageProcessor;
    this.postExecutionMessageProcessor = this.discardMessageProcessor;
    this.preExecutionMessageProcessor = this.discardMessageProcessor;
  }

  public RqueueMessageTemplate getRqueueMessageTemplate() {
    return rqueueMessageTemplate;
  }

  public long getMaxWorkerWaitTime() {
    return maxWorkerWaitTime;
  }

  public void setMaxWorkerWaitTime(long stopTime) {
    maxWorkerWaitTime = stopTime;
  }

  public String getBeanName() {
    return this.beanName;
  }

  @Override
  public void setBeanName(String name) {
    this.beanName = name;
  }

  public RqueueMessageHandler getRqueueMessageHandler() {
    return rqueueMessageHandler;
  }

  public Integer getMaxNumWorkers() {
    return maxNumWorkers;
  }

  public void setMaxNumWorkers(int maxNumWorkers) {
    this.maxNumWorkers = maxNumWorkers;
  }

  public long getBackOffTime() {
    return backOffTime;
  }

  public void setBackOffTime(long backOffTime) {
    this.backOffTime = backOffTime;
  }

  @Override
  public void destroy() throws Exception {
    synchronized (lifecycleMgr) {
      stop();
      doDestroy();
    }
  }

  protected void doDestroy() {
    if (defaultTaskExecutor && taskExecutor != null) {
      ((ThreadPoolTaskExecutor) taskExecutor).destroy();
    }
    if (spinningTaskExecutor != null) {
      ((ThreadPoolTaskExecutor) spinningTaskExecutor).destroy();
    }
  }

  @Override
  public int getPhase() {
    return phase;
  }

  /**
   * Configure a custom phase for the container to start. This allows to start other beans that also
   * implements the {@link SmartLifecycle} interface.
   *
   * @param phase - the phase that defines the phase respecting the {@link
   *     org.springframework.core.Ordered} semantics
   */
  public void setPhase(int phase) {
    this.phase = phase;
  }

  @Override
  public boolean isAutoStartup() {
    return autoStartup;
  }

  /**
   * Control if this component should get started automatically or manually.
   *
   * <p>A value of {@code false} indicates that the container is intended to be started and stopped
   * through an explicit call to {@link #start()} and {@link #stop()}, and analogous to a plain
   * {@link Lifecycle} implementation.
   *
   * @param autoStartup true/false
   * @see #start()
   * @see #stop()
   * @see Lifecycle#stop()
   * @see Lifecycle#start() ()
   */
  public void setAutoStartup(boolean autoStartup) {
    this.autoStartup = autoStartup;
  }

  @Override
  public void stop(Runnable callback) {
    synchronized (this.lifecycleMgr) {
      stop();
      callback.run();
    }
  }

  @Override
  public void afterPropertiesSet() throws Exception {
    synchronized (lifecycleMgr) {
      for (MappingInformation mappingInformation :
          rqueueMessageHandler.getHandlerMethods().keySet()) {
        for (String queue : mappingInformation.getQueueNames()) {
          QueueDetail queueDetail = getQueueDetail(queue, mappingInformation);
          registeredQueues.put(queue, queueDetail);
        }
      }
      registeredQueues = Collections.unmodifiableMap(registeredQueues);
      if (taskExecutor == null) {
        defaultTaskExecutor = true;
        taskExecutor = createDefaultTaskExecutor();
      } else {
        spinningTaskExecutor = createSpinningTaskExecutor();
      }
      initializeRunningQueueState();
      this.semaphore = new Semaphore(getWorkersCount());
      lifecycleMgr.notifyAll();
    }
  }

  protected AsyncTaskExecutor getSpinningTaskExecutor() {
    return spinningTaskExecutor;
  }

  private AsyncTaskExecutor createSpinningTaskExecutor() {
    return createTaskExecutor(true);
  }

  public Map<String, QueueDetail> getRegisteredQueues() {
    return registeredQueues;
  }

  private void initializeRunningQueueState() {
    for (String queue : getRegisteredQueues().keySet()) {
      queueRunningState.put(queue, false);
    }
  }

  private int getWorkersCount() {
    return (maxNumWorkers == null
        ? getRegisteredQueues().size() * DEFAULT_WORKER_COUNT_PER_QUEUE
        : maxNumWorkers);
  }

  private AsyncTaskExecutor createTaskExecutor(boolean onlySpinningThread) {
    String name = getBeanName();
    ThreadPoolTaskExecutor threadPoolTaskExecutor = new ThreadPoolTaskExecutor();
    threadPoolTaskExecutor.setThreadNamePrefix(
        name != null ? name + "-" : DEFAULT_THREAD_NAME_PREFIX);
    threadPoolTaskExecutor.setBeanName(DEFAULT_THREAD_NAME_PREFIX);
    ThreadCount threadCount =
        ThreadUtils.getThreadCount(
            onlySpinningThread, getRegisteredQueues().size(), getWorkersCount());
    if (threadCount.getCorePoolSize() > 0) {
      threadPoolTaskExecutor.setCorePoolSize(threadCount.getCorePoolSize());
      threadPoolTaskExecutor.setMaxPoolSize(threadCount.getMaxPoolSize());
    }
    threadPoolTaskExecutor.setQueueCapacity(0);
    threadPoolTaskExecutor.afterPropertiesSet();
    return threadPoolTaskExecutor;
  }

  public AsyncTaskExecutor createDefaultTaskExecutor() {
    return createTaskExecutor(false);
  }

  private QueueDetail getQueueDetail(String queue, MappingInformation mappingInformation) {
    int numRetries = mappingInformation.getNumRetries();
    if (!mappingInformation.getDeadLetterQueueName().isEmpty() && numRetries == -1) {
      numRetries = Constants.DEFAULT_RETRY_DEAD_LETTER_QUEUE;
    } else if (numRetries == -1) {
      numRetries = Integer.MAX_VALUE;
    }
    return new QueueDetail(
        queue,
        numRetries,
        mappingInformation.getDeadLetterQueueName(),
        mappingInformation.isDelayedQueue(),
        mappingInformation.getVisibilityTimeout());
  }

  @Override
  public void start() {
    log.info("Starting Rqueue Message container");
    synchronized (lifecycleMgr) {
      running = true;
      doStart();
      applicationEventPublisher.publishEvent(
          new QueueInitializationEvent("Container", registeredQueues, true));
      lifecycleMgr.notifyAll();
    }
  }

  protected void doStart() {
    for (Map.Entry<String, QueueDetail> registeredQueue : getRegisteredQueues().entrySet()) {
      QueueDetail queueDetail = registeredQueue.getValue();
      startQueue(registeredQueue.getKey(), queueDetail);
    }
  }

  protected void startQueue(String queueName, QueueDetail queueDetail) {
    if (Boolean.TRUE.equals(queueRunningState.get(queueName))) {
      return;
    }
    queueRunningState.put(queueName, true);
    Future<?> future;
    MessagePoller messagePoller = new MessagePoller(queueName, queueDetail, this, semaphore);
    if (spinningTaskExecutor == null) {
      future = getTaskExecutor().submit(messagePoller);
    } else {
      future = spinningTaskExecutor.submit(messagePoller);
    }
    scheduledFutureByQueue.put(queueName, future);
  }

  boolean isQueueActive(String queueName) {
    return queueRunningState.getOrDefault(queueName, false);
  }

  public AsyncTaskExecutor getTaskExecutor() {
    return taskExecutor;
  }

  public void setTaskExecutor(AsyncTaskExecutor taskExecutor) {
    this.taskExecutor = taskExecutor;
  }

  @Override
  public void stop() {
    log.info("Stopping Rqueue Message container");
    synchronized (lifecycleMgr) {
      running = false;
      applicationEventPublisher.publishEvent(
          new QueueInitializationEvent("Container", registeredQueues, false));
      doStop();
      lifecycleMgr.notifyAll();
    }
  }

  protected void doStop() {
    for (Map.Entry<String, Boolean> runningStateByQueue : queueRunningState.entrySet()) {
      if (Boolean.TRUE.equals(runningStateByQueue.getValue())) {
        stopQueue(runningStateByQueue.getKey());
      }
    }
    waitForRunningQueuesToStop();
  }

  private void waitForRunningQueuesToStop() {
    for (Map.Entry<String, Boolean> entry : queueRunningState.entrySet()) {
      String queueName = entry.getKey();
      Future<?> queueSpinningThread = scheduledFutureByQueue.get(queueName);
      ThreadUtils.waitForTermination(
          log,
          queueSpinningThread,
          getMaxWorkerWaitTime(),
          "An exception occurred while stopping queue '{}'",
          queueName);
    }
  }

  private void stopQueue(String queueName) {
    Assert.isTrue(
        queueRunningState.containsKey(queueName),
        "Queue with name '" + queueName + "' does not exist");
    queueRunningState.put(queueName, false);
  }

  @Override
  public boolean isRunning() {
    synchronized (lifecycleMgr) {
      return running;
    }
  }

  public long getPollingInterval() {
    return pollingInterval;
  }

  public void setPollingInterval(long pollingInterval) {
    this.pollingInterval = pollingInterval;
  }

  public MessageProcessor getDiscardMessageProcessor() {
    return this.discardMessageProcessor;
  }

  public void setDiscardMessageProcessor(MessageProcessor discardMessageProcessor) {
    notNull(discardMessageProcessor, "discardMessageProcessor cannot be null");
    this.discardMessageProcessor = discardMessageProcessor;
  }

  public MessageProcessor getDeadLetterQueueMessageProcessor() {
    return this.deadLetterQueueMessageProcessor;
  }

  public void setDeadLetterQueueMessageProcessor(MessageProcessor deadLetterQueueMessageProcessor) {
    notNull(deadLetterQueueMessageProcessor, "deadLetterQueueMessageProcessor cannot be null");
    this.deadLetterQueueMessageProcessor = deadLetterQueueMessageProcessor;
  }

  RqueueMessageMetadataService getRqueueMessageMetadataService() {
    return rqueueMessageMetadataService;
  }

  public MessageProcessor getManualDeletionMessageProcessor() {
    return this.manualDeletionMessageProcessor;
  }

  public void setManualDeletionMessageProcessor(MessageProcessor manualDeletionMessageProcessor) {
    notNull(manualDeletionMessageProcessor, "manualDeletionMessageProcessor cannot be null");
    this.manualDeletionMessageProcessor = manualDeletionMessageProcessor;
  }

  public MessageProcessor getPostExecutionMessageProcessor() {
    return this.postExecutionMessageProcessor;
  }

  public void setPostExecutionMessageProcessor(MessageProcessor postExecutionMessageProcessor) {
    notNull(postExecutionMessageProcessor, "postExecutionMessageProcessor cannot be null");
    this.postExecutionMessageProcessor = postExecutionMessageProcessor;
  }

  public MessageProcessor getPreExecutionMessageProcessor() {
    return preExecutionMessageProcessor;
  }

  public void setPreExecutionMessageProcessor(MessageProcessor preExecutionMessageProcessor) {
    notNull(preExecutionMessageProcessor, "preExecutionMessageProcessor cannot be null");
    this.preExecutionMessageProcessor = preExecutionMessageProcessor;
  }

  public RqueueCounter getRqueueCounter() {
    return rqueueCounter;
  }

  public RqueueWebConfig getRqueueWebConfig() {
    return rqueueWebConfig;
  }

  public ApplicationEventPublisher getApplicationEventPublisher() {
    return applicationEventPublisher;
  }
}
