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
import static com.github.sonus21.rqueue.utils.ThreadUtils.waitForTermination;
import static org.springframework.util.Assert.notNull;

import com.github.sonus21.rqueue.config.RqueueConfig;
import com.github.sonus21.rqueue.config.RqueueWebConfig;
import com.github.sonus21.rqueue.core.QueueRegistry;
import com.github.sonus21.rqueue.core.RqueueMessageTemplate;
import com.github.sonus21.rqueue.core.support.MessageProcessor;
import com.github.sonus21.rqueue.metrics.RqueueCounter;
import com.github.sonus21.rqueue.models.MinMax;
import com.github.sonus21.rqueue.models.event.RqueueBootstrapEvent;
import com.github.sonus21.rqueue.utils.Constants;
import com.github.sonus21.rqueue.utils.ThreadUtils;
import com.github.sonus21.rqueue.utils.ThreadUtils.QueueThread;
import com.github.sonus21.rqueue.utils.backoff.FixedTaskExecutionBackOff;
import com.github.sonus21.rqueue.utils.backoff.TaskExecutionBackOff;
import com.github.sonus21.rqueue.web.service.RqueueMessageMetadataService;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
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
  private TaskExecutionBackOff taskExecutionBackOff = new FixedTaskExecutionBackOff();
  @Autowired private ApplicationEventPublisher applicationEventPublisher;
  @Autowired private RqueueWebConfig rqueueWebConfig;
  @Autowired private RqueueConfig rqueueConfig;

  @Autowired(required = false)
  private RqueueCounter rqueueCounter;

  @Autowired private RqueueMessageMetadataService rqueueMessageMetadataService;
  private AsyncTaskExecutor taskExecutor;
  private Map<String, QueueThread> queueThreadMap = new ConcurrentHashMap<>();
  private Map<String, Boolean> queueRunningState = new ConcurrentHashMap<>();
  private ConcurrentHashMap<String, Future<?>> scheduledFutureByQueue = new ConcurrentHashMap<>();
  private Integer maxNumWorkers;
  private String beanName;
  private boolean defaultTaskExecutor = false;
  private boolean autoStartup = true;
  private boolean running = false;
  private long backOffTime = 5 * Constants.ONE_MILLI;
  private long maxWorkerWaitTime = 20 * Constants.ONE_MILLI;
  private long pollingInterval = 200L;
  private int phase = Integer.MAX_VALUE;
  private boolean active = true;

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
      if (active) {
        stop();
        doDestroy();
      } else {
        running = false;
      }
    }
  }

  protected void doDestroy() {
    if (defaultTaskExecutor && taskExecutor != null) {
      ((ThreadPoolTaskExecutor) taskExecutor).destroy();
    }
    for (Entry<String, QueueThread> entry : queueThreadMap.entrySet()) {
      QueueThread queueThread = entry.getValue();
      if (queueThread.isDefaultExecutor()) {
        ((ThreadPoolTaskExecutor) queueThread.getTaskExecutor()).destroy();
      }
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
      QueueRegistry.delete();
      for (MappingInformation mappingInformation :
          rqueueMessageHandler.getHandlerMethods().keySet()) {
        for (String queue : mappingInformation.getQueueNames()) {
          QueueDetail queueDetail = getQueueDetail(queue, mappingInformation);
          QueueRegistry.register(queueDetail);
        }
      }
      if (QueueRegistry.getActiveQueueCount() == 0) {
        active = false;
        return;
      }
      if (taskExecutor == null) {
        defaultTaskExecutor = true;
        taskExecutor = createDefaultTaskExecutor();
      } else {
        initializeDefaultExecutor(false, taskExecutor, getMaxNumWorkers());
      }
      initializeRunningQueueState();
      lifecycleMgr.notifyAll();
    }
  }

  private void initializeDefaultExecutor(
      boolean defaultExecutor, AsyncTaskExecutor taskExecutor, int workersCount) {
    Semaphore semaphore = new Semaphore(workersCount);
    for (QueueDetail queueDetail : QueueRegistry.getActiveQueueDetails()) {
      queueThreadMap.put(
          queueDetail.getName(),
          new QueueThread(defaultExecutor, taskExecutor, semaphore, workersCount));
    }
  }

  private void initializeRunningQueueState() {
    for (String queue : QueueRegistry.getActiveQueues()) {
      queueRunningState.put(queue, false);
    }
  }

  private int getWorkersCount() {
    return (maxNumWorkers == null
        ? QueueRegistry.getActiveQueueCount() * DEFAULT_WORKER_COUNT_PER_QUEUE
        : maxNumWorkers);
  }

  private AsyncTaskExecutor createTaskExecutor(int corePoolSize, int maxPoolSize) {
    String name = getBeanName();
    String prefix = name != null ? name + "-" : DEFAULT_THREAD_NAME_PREFIX;
    return ThreadUtils.createTaskExecutor(
        DEFAULT_THREAD_NAME_PREFIX, prefix, corePoolSize, maxPoolSize);
  }

  private AsyncTaskExecutor createNonConcurrencyBasedExecutor(List<QueueDetail> queueDetails) {
    int workersCount = getWorkersCount();
    int maxPoolSize = workersCount + queueDetails.size();
    // one thread for message poller and one for executor
    int corePoolSize = 2 * queueDetails.size();
    AsyncTaskExecutor executor = createTaskExecutor(corePoolSize, maxPoolSize);
    initializeDefaultExecutor(true, executor, workersCount);
    return executor;
  }

  private void createExecutor(QueueDetail queueDetail) {
    MinMax<Integer> concurrency = queueDetail.getConcurrency();
    AsyncTaskExecutor executor =
        createTaskExecutor(queueDetail, concurrency.getMin(), concurrency.getMax());
    Semaphore semaphore = new Semaphore(concurrency.getMax());
    queueThreadMap.put(
        queueDetail.getName(), new QueueThread(true, executor, semaphore, concurrency.getMax()));
  }

  public AsyncTaskExecutor createDefaultTaskExecutor() {
    int withConcurrency = 0;
    List<QueueDetail> queueDetails = QueueRegistry.getActiveQueueDetails();
    for (QueueDetail queueDetail : QueueRegistry.getActiveQueueDetails()) {
      if (queueDetail.getConcurrency().getMin() > 0) {
        withConcurrency += 1;
      }
    }
    if (withConcurrency == 0) {
      return createNonConcurrencyBasedExecutor(queueDetails);
    }
    int remainingWorkers = 0;
    for (QueueDetail queueDetail : QueueRegistry.getActiveQueueDetails()) {
      if (queueDetail.getConcurrency().getMin() > 0) {
        createExecutor(queueDetail);
      } else {
        remainingWorkers += 1;
      }
    }
    int workersCount = remainingWorkers * DEFAULT_WORKER_COUNT_PER_QUEUE;
    int corePoolSize = queueDetails.size() + remainingWorkers;
    int maxPoolSize = queueDetails.size() + workersCount;
    AsyncTaskExecutor executor = createTaskExecutor(corePoolSize, maxPoolSize);
    Semaphore semaphore = new Semaphore(workersCount);
    for (QueueDetail queueDetail : queueDetails) {
      if (queueDetail.getConcurrency().getMin() < 0) {
        queueThreadMap.put(
            queueDetail.getName(), new QueueThread(true, executor, semaphore, workersCount));
      }
    }
    return executor;
  }

  private AsyncTaskExecutor createTaskExecutor(
      QueueDetail queueDetail, int corePoolSize, int maxPoolSize) {
    String name = ThreadUtils.getWorkerName(queueDetail.getName());
    return ThreadUtils.createTaskExecutor(name, name + "-", corePoolSize, maxPoolSize);
  }

  private QueueDetail getQueueDetail(String queue, MappingInformation mappingInformation) {
    int numRetries = mappingInformation.getNumRetries();
    if (!mappingInformation.getDeadLetterQueueName().isEmpty() && numRetries == -1) {
      numRetries = Constants.DEFAULT_RETRY_DEAD_LETTER_QUEUE;
    } else if (numRetries == -1) {
      numRetries = Integer.MAX_VALUE;
    }
    return QueueDetail.builder()
        .name(queue)
        .queueName(rqueueConfig.getQueueName(queue))
        .processingQueueName(rqueueConfig.getProcessingQueueName(queue))
        .delayedQueueName(rqueueConfig.getDelayedQueueName(queue))
        .processingQueueChannelName(rqueueConfig.getProcessingQueueChannelName(queue))
        .delayedQueueChannelName(rqueueConfig.getDelayedQueueChannelName(queue))
        .deadLetterQueueName(mappingInformation.getDeadLetterQueueName())
        .visibilityTimeout(mappingInformation.getVisibilityTimeout())
        .concurrency(mappingInformation.getConcurrency())
        .active(mappingInformation.isActive())
        .numRetry(numRetries)
        .build();
  }

  @Override
  public void start() {
    log.info("Starting Rqueue Message container");
    synchronized (lifecycleMgr) {
      running = true;
      if (active) {
        doStart();
        applicationEventPublisher.publishEvent(new RqueueBootstrapEvent("Container", true));
        lifecycleMgr.notifyAll();
      }
    }
  }

  protected void doStart() {
    for (QueueDetail queueDetail : QueueRegistry.getActiveQueueDetails()) {
      startQueue(queueDetail.getName(), queueDetail);
    }
  }

  protected void startQueue(String queueName, QueueDetail queueDetail) {
    if (Boolean.TRUE.equals(queueRunningState.get(queueName))) {
      return;
    }
    queueRunningState.put(queueName, true);
    QueueThread queueThread = queueThreadMap.get(queueName);
    MessagePoller messagePoller =
        new MessagePoller(
            queueDetail,
            this,
            queueThread.getSemaphore(),
            queueThread.getTaskExecutor(),
            rqueueConfig.getRetriesPerPoll(),
            taskExecutionBackOff);
    Future<?> future = getTaskExecutor().submit(messagePoller);
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
      if (active) {
        applicationEventPublisher.publishEvent(new RqueueBootstrapEvent("Container", false));
        doStop();
        lifecycleMgr.notifyAll();
      }
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
      waitForTermination(
          log,
          queueSpinningThread,
          getMaxWorkerWaitTime(),
          "An exception occurred while stopping queue '{}'",
          queueName);
    }
    if (!waitForTermination(queueThreadMap.values(), getMaxWorkerWaitTime())) {
      log.error("Some workers are not stopped within time");
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

  public void setTaskExecutionBackOff(TaskExecutionBackOff taskExecutionBackOff) {
    notNull(taskExecutionBackOff, "taskExecutionBackOff cannot be null");
    this.taskExecutionBackOff = taskExecutionBackOff;
  }

  public TaskExecutionBackOff getTaskExecutionBackOff() {
    return taskExecutionBackOff;
  }
}
