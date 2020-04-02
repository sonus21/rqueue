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

import com.github.sonus21.rqueue.core.RqueueMessageTemplate;
import com.github.sonus21.rqueue.metrics.RqueueCounter;
import com.github.sonus21.rqueue.processor.MessageProcessor;
import com.github.sonus21.rqueue.utils.Constants;
import com.github.sonus21.rqueue.utils.QueueInitializationEvent;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.BeanNameAware;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.context.SmartLifecycle;
import org.springframework.core.task.AsyncTaskExecutor;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.util.Assert;
import org.springframework.util.ClassUtils;

public class RqueueMessageListenerContainer
    implements InitializingBean, DisposableBean, SmartLifecycle, BeanNameAware {
  private static final String DEFAULT_THREAD_NAME_PREFIX =
      ClassUtils.getShortName(RqueueMessageListenerContainer.class);
  static Logger logger = LoggerFactory.getLogger(RqueueMessageListenerContainer.class);
  private final Object lifecycleMgr = new Object();
  private final RqueueMessageHandler rqueueMessageHandler;
  private final MessageProcessor discardMessageProcessor;
  private final MessageProcessor dlqMessageProcessor;
  private final RqueueMessageTemplate rqueueMessageTemplate;
  private final long maxJobExecutionTime;

  @Autowired(required = false)
  RqueueCounter rqueueCounter;

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
  private int phase = Integer.MAX_VALUE;
  @Autowired private ApplicationEventPublisher applicationEventPublisher;

  public RqueueMessageListenerContainer(
      RqueueMessageHandler rqueueMessageHandler,
      RqueueMessageTemplate rqueueMessageTemplate,
      MessageProcessor discardMessageProcessor,
      MessageProcessor dlqMessageProcessor,
      long maxJobExecutionTime) {
    Assert.notNull(rqueueMessageHandler, "rqueueMessageHandler cannot be null");
    Assert.notNull(rqueueMessageTemplate, "rqueueMessageTemplate cannot be null");
    Assert.notNull(discardMessageProcessor, "discardMessageProcessor cannot be null");
    Assert.notNull(dlqMessageProcessor, "dlqMessageProcessor cannot be null");
    this.rqueueMessageHandler = rqueueMessageHandler;
    this.rqueueMessageTemplate = rqueueMessageTemplate;
    this.discardMessageProcessor = discardMessageProcessor;
    this.dlqMessageProcessor = dlqMessageProcessor;
    this.maxJobExecutionTime = maxJobExecutionTime;
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
      lifecycleMgr.notifyAll();
    }
    if (taskExecutor == null) {
      defaultTaskExecutor = true;
      taskExecutor = createDefaultTaskExecutor();
    } else {
      spinningTaskExecutor = createSpinningTaskExecutor();
    }
    initializeRunningQueueState();
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

  private ThreadCount getThreadCount(boolean onlySpinning) {
    int queueSize = getRegisteredQueues().size();
    int corePoolSize = onlySpinning ? queueSize : queueSize + queueSize;
    int maxPoolSize =
        onlySpinning
            ? queueSize
            : queueSize
                + (getMaxNumWorkers() == null
                    ? queueSize * DEFAULT_WORKER_COUNT_PER_QUEUE
                    : getMaxNumWorkers());
    return new ThreadCount(corePoolSize, maxPoolSize);
  }

  private AsyncTaskExecutor createTaskExecutor(boolean onlySpinningThread) {
    String name = getBeanName();
    ThreadPoolTaskExecutor threadPoolTaskExecutor = new ThreadPoolTaskExecutor();
    threadPoolTaskExecutor.setThreadNamePrefix(
        name != null ? name + "-" : DEFAULT_THREAD_NAME_PREFIX);
    ThreadCount threadCount = getThreadCount(onlySpinningThread);
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
    return new QueueDetail(
        queue,
        mappingInformation.getNumRetries(),
        mappingInformation.getDeadLetterQueueName(),
        mappingInformation.isDelayedQueue());
  }

  @Override
  public void start() {
    logger.info("Starting Rqueue Message container");
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
    if (queueRunningState.containsKey(queueName) && queueRunningState.get(queueName)) {
      return;
    }
    queueRunningState.put(queueName, true);
    Future<?> future;
    AsynchronousMessageListener messageListener =
        new AsynchronousMessageListener(queueName, queueDetail, this);
    if (spinningTaskExecutor == null) {
      future = getTaskExecutor().submit(messageListener);
    } else {
      future = spinningTaskExecutor.submit(messageListener);
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
    logger.info("Stopping Rqueue Message container");
    synchronized (lifecycleMgr) {
      running = false;
      doStop();
      applicationEventPublisher.publishEvent(
          new QueueInitializationEvent("Container", registeredQueues, false));
      lifecycleMgr.notifyAll();
    }
  }

  protected void doStop() {
    for (Map.Entry<String, Boolean> runningStateByQueue : queueRunningState.entrySet()) {
      if (runningStateByQueue.getValue()) {
        stopQueue(runningStateByQueue.getKey());
      }
    }
    waitForRunningQueuesToStop();
  }

  private void waitForRunningQueuesToStop() {
    for (Map.Entry<String, Boolean> queueRunningState : queueRunningState.entrySet()) {
      String queueName = queueRunningState.getKey();
      QueueDetail queueDetail = getRegisteredQueues().get(queueName);
      Future<?> queueSpinningThread = scheduledFutureByQueue.get(queueName);
      if (queueSpinningThread != null
          && !queueSpinningThread.isDone()
          && !queueSpinningThread.isCancelled()) {
        try {
          queueSpinningThread.get(getMaxWorkerWaitTime(), TimeUnit.MILLISECONDS);
        } catch (ExecutionException | TimeoutException e) {
          logger.warn("An exception occurred while stopping queue '{}'", queueName, e);
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
        }
      }
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

  public long getMaxJobExecutionTime() {
    return maxJobExecutionTime;
  }

  public MessageProcessor getDiscardMessageProcessor() {
    return discardMessageProcessor;
  }

  public MessageProcessor getDlqMessageProcessor() {
    return dlqMessageProcessor;
  }
}
