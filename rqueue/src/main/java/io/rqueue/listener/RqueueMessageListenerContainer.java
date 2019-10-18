package io.rqueue.listener;

import io.rqueue.core.LockManager;
import io.rqueue.core.RqueueMessage;
import io.rqueue.core.RqueueMessageTemplate;
import io.rqueue.core.StringMessageTemplate;
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
  private AsyncTaskExecutor taskExecutor;
  private boolean active = false;
  private boolean autoStartup = true;
  private Map<String, ConsumerQueueDetail> registeredQueues = new ConcurrentHashMap<>();
  private Map<String, Boolean> queueRunningState;
  private ConcurrentHashMap<String, Future<?>> scheduledFutureByQueue = new ConcurrentHashMap<>();
  private LockManager lockManager;
  private boolean running = false;
  // 5 seconds
  private long backOffTime = 5000;
  // 20 seconds
  private long maxWorkerWaitTime = 200000;
  // 100 ms
  private long delayedQueueSleepTime = 100;

  public long getDelayedQueueSleepTime() {
    return this.delayedQueueSleepTime;
  }

  public void setDelayedQueueSleepTime(long delayedQueueSleepTime) {
    this.delayedQueueSleepTime = delayedQueueSleepTime;
  }

  public RqueueMessageListenerContainer(
      RqueueMessageTemplate rqueueMessageTemplate,
      RqueueMessageHandler rqueueMessageHandler,
      StringMessageTemplate stringMessageTemplate) {
    this.rqueueMessageHandler = rqueueMessageHandler;
    this.rqueueMessageTemplate = rqueueMessageTemplate;
    this.lockManager = new LockManager(stringMessageTemplate);
  }

  public void setMaxWorkerWaitTime(long stopTime) {
    this.maxWorkerWaitTime = stopTime;
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

  public void setRqueueMessageHandler(RqueueMessageHandler rqueueMessageHandler) {
    if (rqueueMessageHandler == null) {
      throw new IllegalArgumentException("rqueueMessageHandler can not be null");
    }
    this.rqueueMessageHandler = rqueueMessageHandler;
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

  public long getQueueStopTimeout() {
    return this.maxWorkerWaitTime;
  }

  @Override
  public void destroy() throws Exception {
    synchronized (this.lifecycleMgr) {
      this.stop();
      this.active = false;
      if (taskExecutor != null) {
        ((ThreadPoolTaskExecutor) this.taskExecutor).destroy();
      }
    }
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
      this.taskExecutor = createDefaultTaskExecutor();
    }
    initializeRunningQueueState();
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

  private AsyncTaskExecutor createDefaultTaskExecutor() {
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
    Future<?> future =
        getTaskExecutor().submit(new AsynchronousMessageListener(queueName, queueDetail));
    this.scheduledFutureByQueue.put(queueName, future);

    if (queueDetail.isDelayedQueue()) {
      Future<?> future2 = getTaskExecutor().submit(new AsynchronousMessageMover(queueDetail));
      this.scheduledFutureByQueue.put(queueDetail.getZsetName(), future2);
    }
  }

  private boolean isQueueActive(String queueName) {
    return queueRunningState.getOrDefault(queueName, false);
  }

  private AsyncTaskExecutor getTaskExecutor() {
    return taskExecutor;
  }

  @Override
  public void stop() {
    logger.info("Stopping Rqueue Message container");

    synchronized (lifecycleMgr) {
      this.running = false;
      lifecycleMgr.notifyAll();
    }
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
          queueSpinningThread.get(getQueueStopTimeout(), TimeUnit.MILLISECONDS);
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
            messageMoverThread.get(getQueueStopTimeout(), TimeUnit.MILLISECONDS);
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

    public AsynchronousMessageMover(ConsumerQueueDetail consumerQueueDetail) {
      this.queueDetail = consumerQueueDetail;
    }

    @Override
    public void run() {
      while (isQueueActive(queueDetail.getQueueName())) {
        try {
          RqueueMessage rqueueMessage =
              rqueueMessageTemplate.getFromZset(queueDetail.getZsetName());
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
