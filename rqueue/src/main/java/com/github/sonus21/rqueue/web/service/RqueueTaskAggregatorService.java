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

package com.github.sonus21.rqueue.web.service;

import com.github.sonus21.rqueue.common.RqueueLockManager;
import com.github.sonus21.rqueue.config.RqueueConfig;
import com.github.sonus21.rqueue.config.RqueueWebConfig;
import com.github.sonus21.rqueue.core.RqueueMessage;
import com.github.sonus21.rqueue.models.aggregator.QueueEvents;
import com.github.sonus21.rqueue.models.aggregator.TasksStat;
import com.github.sonus21.rqueue.models.db.MessageMetadata;
import com.github.sonus21.rqueue.models.db.QueueStatistics;
import com.github.sonus21.rqueue.models.db.TaskStatus;
import com.github.sonus21.rqueue.models.event.QueueTaskEvent;
import com.github.sonus21.rqueue.utils.Constants;
import com.github.sonus21.rqueue.utils.DateTimeUtils;
import com.github.sonus21.rqueue.utils.ThreadUtils;
import com.github.sonus21.rqueue.utils.TimeoutUtils;
import com.github.sonus21.rqueue.web.dao.RqueueQStatsDao;
import java.time.Duration;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingDeque;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationListener;
import org.springframework.context.SmartLifecycle;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;
import org.springframework.stereotype.Service;
import org.springframework.util.CollectionUtils;

@Service
@Slf4j
public class RqueueTaskAggregatorService
    implements ApplicationListener<QueueTaskEvent>, DisposableBean, SmartLifecycle {
  private final RqueueConfig rqueueConfig;
  private final RqueueWebConfig rqueueWebConfig;
  private final RqueueLockManager rqueueLockManager;
  private final RqueueQStatsDao rqueueQStatsDao;
  private final Object lifecycleMgr = new Object();
  private final Object aggregatorLock = new Object();
  private volatile boolean running = false;
  private ThreadPoolTaskScheduler taskExecutor;
  private Map<String, QueueEvents> queueNameToEvents;
  private BlockingQueue<QueueEvents> queue;
  private List<Future<?>> eventAggregatorTasks;

  @Autowired
  public RqueueTaskAggregatorService(
      RqueueConfig rqueueConfig,
      RqueueWebConfig rqueueWebConfig,
      RqueueLockManager rqueueLockManager,
      RqueueQStatsDao rqueueQStatsDao) {
    this.rqueueConfig = rqueueConfig;
    this.rqueueWebConfig = rqueueWebConfig;
    this.rqueueLockManager = rqueueLockManager;
    this.rqueueQStatsDao = rqueueQStatsDao;
  }

  @Override
  public void destroy() throws Exception {
    log.info("Destroying task aggregator");
    stop();
    if (this.taskExecutor != null) {
      this.taskExecutor.destroy();
    }
  }

  @Override
  public void start() {
    log.info("Starting task aggregation");
    synchronized (lifecycleMgr) {
      running = true;
      if (!rqueueWebConfig.isCollectListenerStats()) {
        return;
      }
      this.eventAggregatorTasks = new ArrayList<>();
      this.queueNameToEvents = new ConcurrentHashMap<>();
      this.queue = new LinkedBlockingDeque<>();
      int threadCount = rqueueWebConfig.getStatsAggregatorThreadCount();
      this.taskExecutor = ThreadUtils.createTaskScheduler(threadCount, "RqueueTaskAggregator-", 30);
      for (int i = 0; i < threadCount; i++) {
        EventAggregator eventAggregator = new EventAggregator();
        eventAggregatorTasks.add(this.taskExecutor.submit(eventAggregator));
      }
      this.taskExecutor.scheduleAtFixedRate(
          new SweepJob(), Duration.ofSeconds(rqueueWebConfig.getAggregateEventWaitTime()));
      lifecycleMgr.notifyAll();
    }
  }

  private boolean processingRequired(QueueEvents queueEvents) {
    return queueEvents.processingRequired(
        rqueueWebConfig.getAggregateEventWaitTime(), rqueueWebConfig.getAggregateEventCount());
  }

  private void waitForRunningTaskToStop() {
    if (!CollectionUtils.isEmpty(eventAggregatorTasks)) {
      for (Future<?> future : eventAggregatorTasks) {
        ThreadUtils.waitForTermination(
            log,
            future,
            rqueueWebConfig.getAggregateShutdownWaitTime(),
            "Aggregator task termination");
      }
    }
  }

  @Override
  public void stop() {
    log.info("Stopping task aggregation");
    synchronized (lifecycleMgr) {
      synchronized (aggregatorLock) {
        if (!CollectionUtils.isEmpty(queueNameToEvents)) {
          Collection<QueueEvents> queueEvents = queueNameToEvents.values();
          queue.addAll(queueEvents);
          queueEvents.clear();
        }
        aggregatorLock.notifyAll();
      }
      running = false;
      waitForRunningTaskToStop();
      lifecycleMgr.notifyAll();
    }
  }

  @Override
  public boolean isRunning() {
    synchronized (lifecycleMgr) {
      return this.running;
    }
  }

  @Override
  public void onApplicationEvent(QueueTaskEvent event) {
    synchronized (aggregatorLock) {
      if (log.isTraceEnabled()) {
        log.trace("Event {}", event);
      }
      String queueName = (String) event.getSource();
      QueueEvents queueEvents = queueNameToEvents.get(queueName);
      if (queueEvents == null) {
        queueEvents = new QueueEvents(event);
      } else {
        queueEvents.addEvent(event);
      }
      if (processingRequired(queueEvents)) {
        if (log.isTraceEnabled()) {
          log.trace("Adding events to the queue");
        }
        queue.add(queueEvents);
        queueNameToEvents.remove(queueName);
      } else {
        queueNameToEvents.put(queueName, queueEvents);
      }
      aggregatorLock.notifyAll();
    }
  }

  class SweepJob implements Runnable {
    @Override
    public void run() {
      if (log.isDebugEnabled()) {
        log.debug("Checking pending events.");
      }
      synchronized (aggregatorLock) {
        List<String> queuesToSweep = new ArrayList<>();
        for (Entry<String, QueueEvents> entry : queueNameToEvents.entrySet()) {
          QueueEvents queueEvents = entry.getValue();
          String queueName = entry.getKey();
          if (processingRequired(queueEvents)) {
            queue.add(queueEvents);
            queuesToSweep.add(queueName);
          }
        }
        for (String queueName : queuesToSweep) {
          queueNameToEvents.remove(queueName);
        }
        aggregatorLock.notifyAll();
      }
    }
  }

  private class EventAggregator implements Runnable {
    private void aggregate(QueueTaskEvent event, TasksStat stat) {
      if (event.getStatus() == TaskStatus.DISCARDED) {
        stat.discarded += 1;
      } else if (event.getStatus() == TaskStatus.SUCCESSFUL) {
        stat.success += 1;
      } else if (event.getStatus() == TaskStatus.MOVED_TO_DLQ) {
        stat.movedToDlq += 1;
      }
      RqueueMessage rqueueMessage = event.getRqueueMessage();
      MessageMetadata messageMetadata = event.getMessageMetadata();
      if (rqueueMessage.getFailureCount() != 0) {
        stat.retried += 1;
      }
      stat.minExecution = Math.min(stat.minExecution, messageMetadata.getTotalExecutionTime());
      stat.maxExecution = Math.max(stat.maxExecution, messageMetadata.getTotalExecutionTime());
      stat.jobCount += 1;
      stat.totalExecutionTime += messageMetadata.getTotalExecutionTime();
    }

    private void aggregate(QueueEvents events) {
      List<QueueTaskEvent> queueTaskEvents = events.taskEvents;
      QueueTaskEvent queueTaskEvent = queueTaskEvents.get(0);
      Map<LocalDate, TasksStat> localDateTasksStatMap = new HashMap<>();
      for (QueueTaskEvent event : queueTaskEvents) {
        LocalDate date = DateTimeUtils.localDateFromMilli(queueTaskEvent.getTimestamp());
        TasksStat stat = localDateTasksStatMap.getOrDefault(date, new TasksStat());
        aggregate(event, stat);
        localDateTasksStatMap.put(date, stat);
      }
      String queueName = (String) queueTaskEvent.getSource();
      String queueStatKey = rqueueConfig.getQueueStatisticsKey(queueName);
      QueueStatistics queueStatistics = rqueueQStatsDao.findById(queueStatKey);
      if (queueStatistics == null) {
        queueStatistics = new QueueStatistics(queueStatKey);
      }
      LocalDate today = DateTimeUtils.today();
      queueStatistics.updateTime();
      for (Entry<LocalDate, TasksStat> entry : localDateTasksStatMap.entrySet()) {
        queueStatistics.update(entry.getValue(), entry.getKey().toString());
      }
      queueStatistics.pruneStats(today, rqueueWebConfig.getHistoryDay());
      rqueueQStatsDao.save(queueStatistics);
    }

    private void processEvents(QueueEvents events) {
      List<QueueTaskEvent> queueTaskEvents = events.taskEvents;
      if (!CollectionUtils.isEmpty(queueTaskEvents)) {
        QueueTaskEvent queueTaskEvent = queueTaskEvents.get(0);
        String queueName = (String) queueTaskEvent.getSource();
        String queueStatKey = rqueueConfig.getQueueStatisticsKey(queueName);
        String lockKey = rqueueConfig.getLockKey(queueStatKey);
        if (rqueueLockManager.acquireLock(
            lockKey, Duration.ofSeconds(Constants.AGGREGATION_LOCK_DURATION_IN_SECONDS))) {
          aggregate(events);
          rqueueLockManager.releaseLock(lockKey);
        } else {
          log.warn("Unable to acquire lock, will retry later");
          queue.add(events);
        }
      }
    }

    @Override
    public void run() {
      while (running) {
        QueueEvents events = null;
        try {
          if (log.isTraceEnabled()) {
            log.trace("Aggregating queue stats");
          }
          events = queue.take();
          processEvents(events);
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
        } catch (Exception e) {
          // unprocessed events
          if (events != null) {
            queue.add(events);
          }
          log.error("Error in aggregator job ", e);
          TimeoutUtils.sleepLog(Constants.MIN_DELAY, false);
        }
      }
    }
  }
}
