/*
 * Copyright (c) 2020-2026 Sonu Kumar
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

package com.github.sonus21.rqueue.web.service;

import static com.google.common.collect.Lists.newArrayList;

import com.github.sonus21.rqueue.config.RqueueConfig;
import com.github.sonus21.rqueue.core.EndpointRegistry;
import com.github.sonus21.rqueue.dao.RqueueStringDao;
import com.github.sonus21.rqueue.dao.RqueueSystemConfigDao;
import com.github.sonus21.rqueue.listener.QueueDetail;
import com.github.sonus21.rqueue.models.db.DeadLetterQueue;
import com.github.sonus21.rqueue.models.db.QueueConfig;
import com.github.sonus21.rqueue.models.event.RqueueBootstrapEvent;
import com.github.sonus21.rqueue.models.response.BaseResponse;
import com.github.sonus21.rqueue.service.RqueueMessageMetadataService;
import com.github.sonus21.rqueue.utils.RetryableRunnable;
import com.github.sonus21.rqueue.web.RqueueSystemManagerService;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;
import org.springframework.util.CollectionUtils;
import reactor.core.publisher.Mono;

@Service
@Slf4j
public class RqueueSystemManagerServiceImpl implements RqueueSystemManagerService {

  private final RqueueConfig rqueueConfig;
  private final RqueueSystemConfigDao rqueueSystemConfigDao;
  private final RqueueMessageMetadataService rqueueMessageMetadataService;

  /**
   * Redis-only DAO for the per-queue data wipe in {@link #deleteQueue(String)}. {@code required
   * = false} so the bean wires on NATS too — there {@code deleteQueue} returns
   * {@code code = 1 "not supported"} since JetStream has no equivalent atomic
   * "delete-these-keys-and-set-this-config" primitive. Source-of-truth for the queue list now
   * comes from {@link EndpointRegistry}, removing the previous Redis-only "set of queue names"
   * key.
   */
  private final RqueueStringDao rqueueStringDao;

  private ScheduledExecutorService executorService;

  @Autowired
  public RqueueSystemManagerServiceImpl(
      RqueueConfig rqueueConfig,
      RqueueSystemConfigDao rqueueSystemConfigDao,
      RqueueMessageMetadataService rqueueMessageMetadataService,
      @Autowired(required = false) RqueueStringDao rqueueStringDao) {
    this.rqueueConfig = rqueueConfig;
    this.rqueueSystemConfigDao = rqueueSystemConfigDao;
    this.rqueueMessageMetadataService = rqueueMessageMetadataService;
    this.rqueueStringDao = rqueueStringDao;
  }

  private List<String> queueKeys(QueueConfig queueConfig) {
    List<String> keys = newArrayList(
        queueConfig.getQueueName(),
        queueConfig.getProcessingQueueName(),
        rqueueConfig.getQueueStatisticsKey(queueConfig.getName()));
    keys.add(queueConfig.getScheduledQueueName());
    if (queueConfig.hasDeadLetterQueue()) {
      for (DeadLetterQueue queue : queueConfig.getDeadLetterQueues()) {
        keys.add(queue.getName());
      }
    }
    return keys;
  }

  @Override
  public BaseResponse deleteQueue(String queueName) {
    QueueConfig queueConfig = rqueueSystemConfigDao.getConfigByName(queueName, true);
    BaseResponse baseResponse = new BaseResponse();
    if (queueConfig == null) {
      baseResponse.setCode(1);
      baseResponse.setMessage("Queue not found");
      return baseResponse;
    }
    if (rqueueStringDao == null) {
      // NATS path: no equivalent atomic "delete-keys-and-set-config" primitive.
      baseResponse.setCode(1);
      baseResponse.setMessage("deleteQueue is not supported on rqueue.backend=nats in v1");
      return baseResponse;
    }
    queueConfig.setDeletedOn(System.currentTimeMillis());
    queueConfig.setDeleted(true);
    rqueueStringDao.deleteAndSet(
        queueKeys(queueConfig), Collections.singletonMap(queueConfig.getId(), queueConfig));
    baseResponse.setCode(0);
    baseResponse.setMessage("Queue deleted");
    return baseResponse;
  }

  private QueueConfig createOrUpdateConfig(QueueConfig queueConfig, QueueDetail queueDetail) {
    String qConfigId = rqueueConfig.getQueueConfigKey(queueDetail.getName());
    QueueConfig systemQueueConfig = queueConfig;
    boolean updated = false;
    boolean created = false;
    if (systemQueueConfig == null) {
      created = true;
      systemQueueConfig = queueDetail.toConfig();
      systemQueueConfig.setId(qConfigId);
    }
    if (queueDetail.isDlqSet()) {
      updated = systemQueueConfig.addDeadLetterQueue(queueDetail.getDeadLetterQueue());
    }

    // for older queue this needs to be set
    if (systemQueueConfig.getScheduledQueueName() == null) {
      systemQueueConfig.setScheduledQueueName(queueDetail.getScheduledQueueName());
      updated = true;
    }

    if (systemQueueConfig.getCompletedQueueName() == null) {
      systemQueueConfig.setCompletedQueueName(queueDetail.getCompletedQueueName());
      updated = true;
    }

    updated =
        systemQueueConfig.updateVisibilityTimeout(queueDetail.getVisibilityTimeout()) || updated;
    updated =
        systemQueueConfig.updateConcurrency(queueDetail.getConcurrency().toMinMax()) || updated;
    updated = systemQueueConfig.updateRetryCount(queueDetail.getNumRetry()) || updated;
    updated = systemQueueConfig.updatePriorityGroup(queueDetail.getPriorityGroup()) || updated;
    updated = systemQueueConfig.updatePriority(queueDetail.getPriority()) || updated;
    if (updated && !created) {
      systemQueueConfig.updateTime();
    }
    if (updated || created) {
      return systemQueueConfig;
    }
    return null;
  }

  private void createOrUpdateConfigs(List<QueueDetail> queueDetails) {
    String[] queues = new String[queueDetails.size()];
    int i = 0;
    for (QueueDetail queueDetail : queueDetails) {
      queues[i++] = queueDetail.getName();
    }
    // The previous Redis "set of queue names" cache (rqueueStringDao.appendToSet) is gone —
    // EndpointRegistry.getActiveQueues() is the in-memory source of truth and works on every
    // backend. The system-config DAO still persists the per-queue metadata below.
    List<String> ids =
        Arrays.stream(queues).map(rqueueConfig::getQueueConfigKey).collect(Collectors.toList());
    List<QueueConfig> queueConfigs = rqueueSystemConfigDao.findAllQConfig(ids);
    List<QueueConfig> newConfigs = new ArrayList<>();
    for (QueueDetail queueDetail : queueDetails) {
      QueueConfig dbConfig = null;
      for (QueueConfig queueConfig : queueConfigs) {
        if (queueConfig.getQueueName().equals(queueDetail.getQueueName())) {
          dbConfig = queueConfig;
          break;
        }
      }
      QueueConfig newConfig = createOrUpdateConfig(dbConfig, queueDetail);
      if (newConfig != null) {
        newConfigs.add(newConfig);
      }
    }
    if (!CollectionUtils.isEmpty(newConfigs)) {
      rqueueSystemConfigDao.saveAllQConfig(newConfigs);
    }
  }

  @Override
  @Async
  public void onApplicationEvent(RqueueBootstrapEvent event) {
    if (event.isStartup()) {
      List<QueueDetail> queueDetails = EndpointRegistry.getActiveQueueDetails();
      if (queueDetails.isEmpty()) {
        return;
      }
      createOrUpdateConfigs(queueDetails);
    }
    handleEventForCleanup(event);
  }

  private void handleEventForCleanup(RqueueBootstrapEvent event) {
    if (executorService != null) {
      executorService.shutdown();
    }
    if (rqueueConfig.isProducer()) {
      return;
    }
    if (event.isStartup() && rqueueConfig.messageInTerminalStateShouldBeStored()) {
      executorService = Executors.newSingleThreadScheduledExecutor();
      List<QueueConfig> queueConfigList =
          rqueueSystemConfigDao.getConfigByNames(EndpointRegistry.getActiveQueues());
      int i = 0;
      for (QueueConfig queueConfig : queueConfigList) {
        CleanCompletedJobs completedJobs = new CleanCompletedJobs(queueConfig.getName());
        executorService.scheduleAtFixedRate(
            completedJobs,
            i,
            rqueueConfig.getCompletedJobCleanupIntervalInMs(),
            TimeUnit.MILLISECONDS);
      }
    }
  }

  @Override
  public List<String> getQueues() {
    // EndpointRegistry is the in-memory source of truth for active queue names; identical
    // semantics to the previous Redis "set of queue names" key on a single instance, and it
    // works on both backends without needing a per-backend Redis-set abstraction.
    return new ArrayList<>(EndpointRegistry.getActiveQueues());
  }

  @Override
  public List<QueueConfig> getQueueConfigs(Collection<String> queues) {
    Collection<String> ids = new ArrayList<>();
    if (!CollectionUtils.isEmpty(queues)) {
      ids = queues.stream().map(rqueueConfig::getQueueConfigKey).collect(Collectors.toList());
    }
    if (!CollectionUtils.isEmpty(ids)) {
      return rqueueSystemConfigDao.findAllQConfig(ids);
    }
    return Collections.emptyList();
  }

  @Override
  public List<QueueConfig> getQueueConfigs() {
    List<String> queues = getQueues();
    return getQueueConfigs(queues);
  }

  @Override
  public List<QueueConfig> getSortedQueueConfigs() {
    List<String> queues = getQueues();
    return getQueueConfigs(queues).stream()
        .sorted(Comparator.comparing(QueueConfig::getName))
        .collect(Collectors.toList());
  }

  @Override
  public QueueConfig getQueueConfig(String queueName) {
    List<QueueConfig> queueConfigs = getQueueConfigs(Collections.singletonList(queueName));
    if (CollectionUtils.isEmpty(queueConfigs)) {
      return null;
    }
    return queueConfigs.get(0);
  }

  @Override
  public Mono<BaseResponse> deleteReactiveQueue(String queueName) {
    return Mono.just(deleteQueue(queueName));
  }

  private class CleanCompletedJobs extends RetryableRunnable<Object> {

    private final String queueName;

    protected CleanCompletedJobs(String queueName) {
      super(log, "");
      this.queueName = queueName;
    }

    @Override
    public void start() {
      QueueConfig queueConfig = rqueueSystemConfigDao.getConfigByName(queueName, true);
      if (queueConfig == null) {
        log.error("Queue config does not exist {}", queueName);
        return;
      }
      if (queueConfig.isPaused()) {
        log.debug("Queue {} is paused", queueName);
        return;
      }
      if (queueConfig.getCompletedQueueName() == null) {
        log.error("Queue completed queue name is not set {}", queueName);
        return;
      }

      long endTime =
          System.currentTimeMillis() - rqueueConfig.messageDurabilityInTerminalStateInMillisecond();
      log.debug("Deleting completed messages for queue: {}, before: {}", endTime, queueName);
      rqueueMessageMetadataService.deleteQueueMessages(
          queueConfig.getCompletedQueueName(), endTime);
    }
  }
}
