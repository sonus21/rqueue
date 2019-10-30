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

import com.github.sonus21.rqueue.core.RqueueMessage;
import com.github.sonus21.rqueue.core.RqueueMessageTemplate;
import com.github.sonus21.rqueue.core.ThreadCount;
import com.github.sonus21.rqueue.utils.QueueInfo;
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
import org.springframework.context.ApplicationContext;
import org.springframework.context.SmartLifecycle;
import org.springframework.core.task.AsyncTaskExecutor;
import org.springframework.data.redis.listener.RedisMessageListenerContainer;
import org.springframework.messaging.Message;
import org.springframework.messaging.support.GenericMessage;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.util.Assert;
import org.springframework.util.ClassUtils;

@SuppressWarnings({"unused"})
public class RqueueMessageListenerContainer
    implements InitializingBean, DisposableBean, SmartLifecycle, BeanNameAware {
  private static final String DEFAULT_THREAD_NAME_PREFIX =
      ClassUtils.getShortName(RqueueMessageListenerContainer.class);
  private static final int DEFAULT_WORKER_COUNT_PER_QUEUE = 2;
  private static final int POOL_SIZE_FOR_MESSAGE_MOVER = 5;
  private static final int POOL_SIZE_FOR_MESSAGE_PROCESSING_MOVER = 2;
  private static Logger logger = LoggerFactory.getLogger(RqueueMessageListenerContainer.class);
  private final Object lifecycleMgr = new Object();
  private Integer maxNumWorkers;
  private String beanName;
  private RqueueMessageHandler rqueueMessageHandler;
  private boolean defaultTaskExecutor = false;
  private AsyncTaskExecutor taskExecutor;
  private AsyncTaskExecutor spinningTaskExecutor;
  private boolean autoStartup = true;
  private Map<String, ConsumerQueueDetail> registeredQueues = new ConcurrentHashMap<>();
  private Map<String, Boolean> queueRunningState = new ConcurrentHashMap<>();
  private ConcurrentHashMap<String, Future<?>> scheduledFutureByQueue = new ConcurrentHashMap<>();
  private boolean running = false;
  // 1 second
  private long backOffTime = 1000;
  // 20 seconds
  private long maxWorkerWaitTime = 200000;
  // 100 ms
  private long delayedQueueSleepTime = 100;
  private int phase = Integer.MAX_VALUE;
  private ApplicationContext applicationContext;
  private RqueueMessageTemplate rqueueMessageTemplate;

  @Autowired private RedisMessageListenerContainer rqueueRedisMessageListenerContainer;

  public long getDelayedQueueSleepTime() {
    return this.delayedQueueSleepTime;
  }

  public void setDelayedQueueSleepTime(long delayedQueueSleepTime) {
    this.delayedQueueSleepTime = delayedQueueSleepTime;
  }

  public RqueueMessageListenerContainer(
      RqueueMessageHandler rqueueMessageHandler, RqueueMessageTemplate rqueueMessageTemplate) {
    Assert.notNull(rqueueMessageHandler, "rqueueMessageHandler can not be null");
    Assert.notNull(rqueueMessageTemplate, "rqueueMessageTemplate can not be null");
    this.rqueueMessageHandler = rqueueMessageHandler;
    this.rqueueMessageTemplate = rqueueMessageTemplate;
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

  public void setTaskExecutor(AsyncTaskExecutor taskExecutor) {
    this.taskExecutor = taskExecutor;
  }

  @Override
  @SuppressWarnings("RedundantThrows")
  public void destroy() throws Exception {
    synchronized (this.lifecycleMgr) {
      this.stop();
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

  @SuppressWarnings("RedundantThrows")
  @Override
  public void afterPropertiesSet() throws Exception {
    int delayedQueueCount = 0;
    synchronized (this.lifecycleMgr) {
      for (MappingInformation mappingInformation :
          rqueueMessageHandler.getHandlerMethods().keySet()) {
        for (String queue : mappingInformation.getQueueNames()) {
          ConsumerQueueDetail consumerQueueDetail =
              getConsumerQueueDetail(queue, mappingInformation);
          if (consumerQueueDetail.isDelayedQueue()) {
            delayedQueueCount += 1;
          }
          this.registeredQueues.put(queue, consumerQueueDetail);
        }
      }
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

  protected AsyncTaskExecutor getSpinningTaskExecutor() {
    return this.spinningTaskExecutor;
  }

  private AsyncTaskExecutor createSpinningTaskExecutor() {
    return createTaskExecutor(true);
  }

  public Map<String, ConsumerQueueDetail> getRegisteredQueues() {
    return Collections.unmodifiableMap(registeredQueues);
  }

  private void initializeRunningQueueState() {
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

  private ThreadCount getThreadCount(boolean onlySpinning) {
    int messageMoverThreadsCount = getDelayedQueueCounts();
    int messagePullerThreadsCount = getRegisteredQueues().size();
    int spinningThreads = messageMoverThreadsCount + messagePullerThreadsCount;
    int corePoolSize = onlySpinning ? spinningThreads : spinningThreads + messagePullerThreadsCount;
    int maxPoolSize =
        onlySpinning
            ? spinningThreads
            : spinningThreads
                + (getMaxNumWorkers() == null
                    ? messagePullerThreadsCount * DEFAULT_WORKER_COUNT_PER_QUEUE
                    : getMaxNumWorkers());
    return new ThreadCount(corePoolSize, maxPoolSize);
  }

  private AsyncTaskExecutor createTaskExecutor(boolean onlySpinningThread) {
    String beanName = getBeanName();
    ThreadPoolTaskExecutor threadPoolTaskExecutor = new ThreadPoolTaskExecutor();
    threadPoolTaskExecutor.setThreadNamePrefix(
        beanName != null ? beanName + "-" : DEFAULT_THREAD_NAME_PREFIX);
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
      ConsumerQueueDetail queueDetail = registeredQueue.getValue();
      startQueue(registeredQueue.getKey(), queueDetail);
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

  private class AsynchronousMessageListener implements Runnable {
    private final String queueName;
    private final ConsumerQueueDetail queueDetail;

    private RqueueMessage getMessage() {
      return rqueueMessageTemplate.pop(queueName);
    }

    AsynchronousMessageListener(String queueName, ConsumerQueueDetail value) {
      this.queueName = queueName;
      this.queueDetail = value;
    }

    @Override
    public void run() {
      while (isQueueActive(queueName)) {
        try {
          RqueueMessage message = getMessage();
          if (message != null) {
            if (isQueueActive(queueName)) {
              getTaskExecutor().submit(new MessageExecutor(message, queueDetail));
            } else {
              rqueueMessageTemplate.add(queueName, message);
            }
          }
        } catch (Exception e) {
          logger.warn(
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

  class MessageExecutor implements Runnable {
    private final ConsumerQueueDetail queueDetail;
    private final Message<String> message;
    private final RqueueMessage rqueueMessage;
    private static final int MAX_RETRY_COUNT = Integer.MAX_VALUE;
    private static final long DELTA_BETWEEN_RE_ENQUEUE_TIME = 5000L;
    private final Long accessTime;

    MessageExecutor(RqueueMessage message, ConsumerQueueDetail queueDetail) {
      this.rqueueMessage = message;
      this.queueDetail = queueDetail;
      this.accessTime = System.currentTimeMillis();
      this.message =
          new GenericMessage<>(
              message.getMessage(), QueueInfo.getQueueHeaders(queueDetail.getQueueName()));
    }

    private int getMaxRetryCount() {
      int maxRetryCount =
          rqueueMessage.getRetryCount() == null
              ? queueDetail.getNumRetries()
              : rqueueMessage.getRetryCount();
      // DLQ is  not specified so retry it for max number of counts
      if (maxRetryCount == -1 && queueDetail.getDlqName().isEmpty()) {
        maxRetryCount = MAX_RETRY_COUNT;
      }
      return maxRetryCount;
    }

    private long getMaxProcessingTime() {
      return QueueInfo.getMessageReEnqueueTime() - DELTA_BETWEEN_RE_ENQUEUE_TIME;
    }

    private void handlePostProcessing(
        boolean executed, int currentFailureCount, int maxRetryCount) {
      if (!isQueueActive(queueDetail.getQueueName())) {
        return;
      }
      try {
        String processingQueueName = QueueInfo.getProcessingQueueName(queueDetail.getQueueName());
        if (!executed) {
          // move to DLQ
          if (!queueDetail.getDlqName().isEmpty()) {
            RqueueMessage newMessage = rqueueMessage.clone();
            newMessage.setFailureCount(currentFailureCount);
            newMessage.setAccessTime(accessTime);
            newMessage.updateReEnqueuedAt();
            rqueueMessageTemplate.add(queueDetail.getDlqName(), newMessage);
            rqueueMessageTemplate.removeFromZset(processingQueueName, rqueueMessage);
          } else if (currentFailureCount < maxRetryCount) {
            // replace the existing message with the update message
            // this will reflect the retry count
            RqueueMessage newMessage = rqueueMessage.clone();
            newMessage.setAccessTime(accessTime);
            newMessage.setFailureCount(currentFailureCount);
            newMessage.updateReEnqueuedAt();
            rqueueMessageTemplate.replaceMessage(processingQueueName, rqueueMessage, newMessage);
          } else {
            // discard this message
            logger.warn("Message {} discarded due to retry limit", rqueueMessage);
            rqueueMessageTemplate.removeFromZset(processingQueueName, rqueueMessage);
          }
        } else {
          // delete it from processing queue
          rqueueMessageTemplate.removeFromZset(processingQueueName, rqueueMessage);
        }
      } catch (Exception e) {
        logger.error("Error occurred in post processing", e);
      }
    }

    @Override
    public void run() {
      boolean executed = false;
      int currentFailureCount = rqueueMessage.getFailureCount();
      int maxRetryCount = getMaxRetryCount();
      long maxRetryTime = getMaxProcessingTime();
      do {
        if (!isQueueActive(queueDetail.getQueueName())) {
          return;
        }
        try {
          getRqueueMessageHandler().handleMessage(message);
          executed = true;
        } catch (Exception e) {
          logger.warn("Message '{}' is failing", message.getPayload(), e);
          currentFailureCount += 1;
        }
      } while (currentFailureCount < maxRetryCount
          && !executed
          && System.currentTimeMillis() < maxRetryTime);
      handlePostProcessing(executed, currentFailureCount, maxRetryCount);
    }
  }
}
