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

package com.github.sonus21.rqueue.listener;

import com.github.sonus21.rqueue.core.LockManager;
import com.github.sonus21.rqueue.core.RqueueMessage;
import com.github.sonus21.rqueue.core.RqueueMessageTemplate;
import com.github.sonus21.rqueue.core.StringMessageTemplate;
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
import org.springframework.context.SmartLifecycle;
import org.springframework.core.task.AsyncTaskExecutor;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.util.Assert;
import org.springframework.util.ClassUtils;

public class RqueueMessageListenerContainer
    implements InitializingBean, DisposableBean, SmartLifecycle, BeanNameAware {
  private static final String DEFAULT_THREAD_NAME_PREFIX =
      ClassUtils.getShortName(RqueueMessageListenerContainer.class);
  private static final int DEFAULT_WORKER_COUNT_PER_QUEUE = 2;
  private static Logger logger = LoggerFactory.getLogger(RqueueMessageListenerContainer.class);
  private final Object lifecycleMgr = new Object();
  private Integer maxNumWorkers;
  private String beanName;
  private RqueueMessageTemplate rqueueMessageTemplate;
  private RqueueMessageHandler rqueueMessageHandler;
  private boolean defaultTaskExecutor = false;
  private AsyncTaskExecutor taskExecutor;
  private AsyncTaskExecutor spinningTaskExecutor;
  private boolean active = false;
  private boolean autoStartup = true;
  private Map<String, ConsumerQueueDetail> registeredQueues = new ConcurrentHashMap<>();
  private Map<String, Boolean> queueRunningState;
  private ConcurrentHashMap<String, Future<?>> scheduledFutureByQueue = new ConcurrentHashMap<>();
  private LockManager lockManager;
  private boolean running = false;
  // 1 second
  private long backOffTime = 1000;
  // 20 seconds
  private long maxWorkerWaitTime = 200000;
  // 100 ms
  private long delayedQueueSleepTime = 100;
  private int phase = Integer.MAX_VALUE;

  public long getDelayedQueueSleepTime() {
    return this.delayedQueueSleepTime;
  }

  public void setDelayedQueueSleepTime(long delayedQueueSleepTime) {
    this.delayedQueueSleepTime = delayedQueueSleepTime;
  }

  public RqueueMessageListenerContainer(
      RqueueMessageHandler rqueueMessageHandler,
      RqueueMessageTemplate rqueueMessageTemplate,
      StringMessageTemplate stringMessageTemplate) {
    Assert.notNull(rqueueMessageHandler, "rqueueMessageHandler can not be null");
    Assert.notNull(rqueueMessageTemplate, "rqueueMessageTemplate can not be null");
    Assert.notNull(stringMessageTemplate, "stringMessageTemplate can not be null");
    this.rqueueMessageHandler = rqueueMessageHandler;
    this.rqueueMessageTemplate = rqueueMessageTemplate;
    this.lockManager = new LockManager(stringMessageTemplate);
  }

  public void setMaxWorkerWaitTime(long stopTime) {
    this.maxWorkerWaitTime = stopTime;
  }

  public long getMaxWorkerWaitTime() {
    return this.maxWorkerWaitTime;
  }

  public String getBeanName() {
    return beanName;
  }

  @Override
  public void setBeanName(String name) {
    this.beanName = name;
  }

  public RqueueMessageHandler getRqueueMessageHandler() {
    return this.rqueueMessageHandler;
  }

  public Integer getMaxNumWorkers() {
    return maxNumWorkers;
  }

  public void setMaxNumWorkers(int maxNumWorkers) {
    this.maxNumWorkers = maxNumWorkers;
  }

  public void setBackOffTime(long backOffTime) {
    this.backOffTime = backOffTime;
  }

  public long getBackoffTime() {
    return backOffTime;
  }

  @Override
  public void destroy() throws Exception {
    synchronized (this.lifecycleMgr) {
      this.stop();
      this.active = false;
      doDestroy();
    }
  }

  protected void doDestroy() {
    if (defaultTaskExecutor && taskExecutor != null) {
      ((ThreadPoolTaskExecutor) this.taskExecutor).destroy();
    }
    if (spinningTaskExecutor != null) {
      ((ThreadPoolTaskExecutor) this.spinningTaskExecutor).destroy();
    }
  }

  @Override
  public int getPhase() {
    return this.phase;
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
  public void afterPropertiesSet() throws Exception {
    synchronized (this.lifecycleMgr) {
      for (MappingInformation mappingInformation :
          rqueueMessageHandler.getHandlerMethods().keySet()) {
        for (String queue : mappingInformation.getQueueNames()) {
          ConsumerQueueDetail consumerQueueDetail =
              getConsumerQueueDetail(queue, mappingInformation);
          this.registeredQueues.put(queue, consumerQueueDetail);
        }
      }
      this.active = true;
      this.lifecycleMgr.notifyAll();
    }
    if (this.taskExecutor == null) {
      defaultTaskExecutor = true;
      this.taskExecutor = createDefaultTaskExecutor();
    } else {
      this.spinningTaskExecutor = createSpinningTaskExecutor();
    }
    initializeRunningQueueState();
  }

  private AsyncTaskExecutor createSpinningTaskExecutor() {
    String beanName = getBeanName();
    ThreadPoolTaskExecutor threadPoolTaskExecutor = new ThreadPoolTaskExecutor();
    threadPoolTaskExecutor.setThreadNamePrefix(
        beanName != null ? beanName + "-" : DEFAULT_THREAD_NAME_PREFIX);
    int messageMoverThreadsCount = getDelayedQueueCounts();
    int messagePullerThreadsCount = getRegisteredQueues().size();
    int spinningThreads = messageMoverThreadsCount + messagePullerThreadsCount;

    if (spinningThreads > 0) {
      threadPoolTaskExecutor.setCorePoolSize(spinningThreads);
      threadPoolTaskExecutor.setMaxPoolSize(2 * spinningThreads);
    }
    threadPoolTaskExecutor.setQueueCapacity(0);
    threadPoolTaskExecutor.afterPropertiesSet();
    return threadPoolTaskExecutor;
  }

  private Map<String, ConsumerQueueDetail> getRegisteredQueues() {
    return Collections.unmodifiableMap(registeredQueues);
  }

  private void initializeRunningQueueState() {
    queueRunningState = new ConcurrentHashMap<>(getRegisteredQueues().size());
    for (String queue : this.registeredQueues.keySet()) {
      queueRunningState.put(queue, false);
    }
  }

  private int getDelayedQueueCounts() {
    return (int)
        this.getRegisteredQueues().values().stream()
            .filter(ConsumerQueueDetail::isDelayedQueue)
            .count();
  }

  public AsyncTaskExecutor createDefaultTaskExecutor() {
    String beanName = getBeanName();
    ThreadPoolTaskExecutor threadPoolTaskExecutor = new ThreadPoolTaskExecutor();
    threadPoolTaskExecutor.setThreadNamePrefix(
        beanName != null ? beanName + "-" : DEFAULT_THREAD_NAME_PREFIX);
    int messageMoverThreadsCount = getDelayedQueueCounts();
    int messagePullerThreadsCount = getRegisteredQueues().size();
    int spinningThreads = messageMoverThreadsCount + messagePullerThreadsCount;

    if (spinningThreads > 0) {
      // one worker for each queue
      threadPoolTaskExecutor.setCorePoolSize(spinningThreads + messagePullerThreadsCount);
      int maxWorkers =
          getMaxNumWorkers() == null
              ? messagePullerThreadsCount * DEFAULT_WORKER_COUNT_PER_QUEUE
              : getMaxNumWorkers();
      threadPoolTaskExecutor.setMaxPoolSize(spinningThreads + maxWorkers);
    }
    threadPoolTaskExecutor.setQueueCapacity(0);
    threadPoolTaskExecutor.afterPropertiesSet();
    return threadPoolTaskExecutor;
  }

  private ConsumerQueueDetail getConsumerQueueDetail(
      String queue, MappingInformation mappingInformation) {
    return new ConsumerQueueDetail(
        queue,
        mappingInformation.getNumRetries(),
        mappingInformation.getDeadLaterQueueName(),
        mappingInformation.isDelayedQueue());
  }

  @Override
  public void start() {
    logger.info("Starting Rqueue Message container");
    synchronized (lifecycleMgr) {
      running = true;
      lifecycleMgr.notifyAll();
    }
    doStart();
  }

  protected void doStart() {
    for (Map.Entry<String, ConsumerQueueDetail> registeredQueue :
        getRegisteredQueues().entrySet()) {
      startQueue(registeredQueue.getKey(), registeredQueue.getValue());
    }
  }

  private void startQueue(String queueName, ConsumerQueueDetail queueDetail) {
    if (this.queueRunningState.containsKey(queueName) && this.queueRunningState.get(queueName)) {
      return;
    }
    this.queueRunningState.put(queueName, true);
    Future<?> future;
    AsynchronousMessageListener messageListener =
        new AsynchronousMessageListener(queueName, queueDetail);
    if (spinningTaskExecutor == null) {
      future = getTaskExecutor().submit(messageListener);
    } else {
      future = spinningTaskExecutor.submit(messageListener);
    }
    this.scheduledFutureByQueue.put(queueName, future);

    if (queueDetail.isDelayedQueue()) {
      AsynchronousMessageMover messageMover = new AsynchronousMessageMover(queueDetail);
      if (spinningTaskExecutor == null) {
        future = getTaskExecutor().submit(messageMover);
      } else {
        future = spinningTaskExecutor.submit(messageMover);
      }
      this.scheduledFutureByQueue.put(queueDetail.getZsetName(), future);
    }
  }

  private boolean isQueueActive(String queueName) {
    return queueRunningState.getOrDefault(queueName, false);
  }

  public AsyncTaskExecutor getTaskExecutor() {
    return taskExecutor;
  }

  @Override
  public void stop() {
    logger.info("Stopping Rqueue Message container");

    synchronized (lifecycleMgr) {
      this.running = false;
      lifecycleMgr.notifyAll();
    }
    doStop();
  }

  protected void doStop() {
    for (Map.Entry<String, Boolean> runningStateByQueue : this.queueRunningState.entrySet()) {
      if (runningStateByQueue.getValue()) {
        stopQueue(runningStateByQueue.getKey());
      }
    }
    waitForRunningQueuesToStop();
  }

  private void waitForRunningQueuesToStop() {
    for (Map.Entry<String, Boolean> queueRunningState : this.queueRunningState.entrySet()) {
      String queueName = queueRunningState.getKey();
      ConsumerQueueDetail queueDetail = this.registeredQueues.get(queueName);
      Future<?> queueSpinningThread = this.scheduledFutureByQueue.get(queueName);
      if (queueSpinningThread != null) {
        try {
          queueSpinningThread.get(getMaxWorkerWaitTime(), TimeUnit.MILLISECONDS);
        } catch (ExecutionException | TimeoutException e) {
          logger.warn("An exception occurred while stopping queue '{}'", queueName, e);
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
        }
      }
      // stop message mover threads
      if (queueDetail.isDelayedQueue()) {
        Future<?> messageMoverThread = this.scheduledFutureByQueue.get(queueDetail.getZsetName());
        if (messageMoverThread != null) {
          try {
            messageMoverThread.get(getMaxWorkerWaitTime(), TimeUnit.MILLISECONDS);
          } catch (ExecutionException | TimeoutException e) {
            logger.warn("An exception occurred while stopping queue '{}'", queueName, e);
          } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
          }
        }
      }
    }
  }

  private void stopQueue(String queueName) {
    Assert.isTrue(
        this.queueRunningState.containsKey(queueName),
        "Queue with name '" + queueName + "' does not exist");
    this.queueRunningState.put(queueName, false);
  }

  @Override
  public boolean isRunning() {
    synchronized (lifecycleMgr) {
      return this.running;
    }
  }

  public void setTaskExecutor(AsyncTaskExecutor taskExecutor) {
    this.taskExecutor = taskExecutor;
  }

  private class AsynchronousMessageMover implements Runnable {
    private final ConsumerQueueDetail queueDetail;

    AsynchronousMessageMover(ConsumerQueueDetail consumerQueueDetail) {
      this.queueDetail = consumerQueueDetail;
    }

    @Override
    public void run() {
      while (isQueueActive(queueDetail.getQueueName())) {
        try {
          RqueueMessage rqueueMessage =
              rqueueMessageTemplate.getFirstFromZset(queueDetail.getZsetName());
          if (rqueueMessage != null && rqueueMessage.getProcessAt() < System.currentTimeMillis()) {
            if (lockManager.acquireLock(rqueueMessage.getId())) {
              // TODO use pipleline here
              rqueueMessageTemplate.add(queueDetail.getQueueName(), rqueueMessage);
              rqueueMessageTemplate.removeFromZset(queueDetail.getZsetName(), rqueueMessage);
            }
            lockManager.releaseLock(rqueueMessage.getId());
          } else {
            try {
              Thread.sleep(getDelayedQueueSleepTime());
            } catch (InterruptedException ex) {
              ex.printStackTrace();
            }
          }
        } catch (Exception e) {
          logger.error(
              "Message mover failed for queue {}, it will be retried in {} Ms",
              queueDetail.getQueueName(),
              getBackoffTime(),
              e);
          try {
            Thread.sleep(getBackoffTime());
          } catch (InterruptedException ex) {
            ex.printStackTrace();
          }
        }
      }
    }
  }

  private class AsynchronousMessageListener implements Runnable {
    private final String queueName;
    private final ConsumerQueueDetail queueDetail;

    private RqueueMessage getMessage() {
      return rqueueMessageTemplate.lpop(queueName);
    }

    AsynchronousMessageListener(String queueName, ConsumerQueueDetail value) {
      this.queueName = queueName;
      this.queueDetail = value;
    }

    @Override
    public void run() {
      try {
        while (isQueueActive(queueName)) {
          RqueueMessage message = getMessage();
          if (message != null) {
            if (isQueueActive(queueName)) {
              getTaskExecutor()
                  .submit(
                      new MessageExecutor(
                          message,
                          queueDetail,
                          getRqueueMessageHandler(),
                          rqueueMessageTemplate,
                          logger));
            } else {
              rqueueMessageTemplate.add(queueName, message);
            }
          }
        }
      } catch (Exception e) {
        logger.error(
            "Message listener failed for queue {}, it will be retried in {} Ms",
            queueName,
            getBackoffTime(),
            e);
        try {
          Thread.sleep(getBackoffTime());
        } catch (InterruptedException ex) {
          ex.printStackTrace();
        }
      }
    }
  }
}
