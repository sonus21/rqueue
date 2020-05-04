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

package com.github.sonus21.rqueue.web.service.impl;

import com.github.sonus21.rqueue.common.RqueueRedisTemplate;
import com.github.sonus21.rqueue.listener.QueueDetail;
import com.github.sonus21.rqueue.models.db.QueueConfig;
import com.github.sonus21.rqueue.models.event.QueueInitializationEvent;
import com.github.sonus21.rqueue.models.response.BaseResponse;
import com.github.sonus21.rqueue.utils.QueueUtils;
import com.github.sonus21.rqueue.utils.RedisUtils;
import com.github.sonus21.rqueue.web.dao.RqueueSystemConfigDao;
import com.github.sonus21.rqueue.web.service.RqueueSystemManagerService;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationListener;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;
import org.springframework.util.CollectionUtils;

@Service
public class RqueueSystemManagerServiceImpl
    implements RqueueSystemManagerService, ApplicationListener<QueueInitializationEvent> {
  private final RqueueRedisTemplate<String> stringRqueueRedisTemplate;
  private final RqueueSystemConfigDao rqueueSystemConfigDao;

  @Autowired
  public RqueueSystemManagerServiceImpl(
      RqueueRedisTemplate<String> stringRqueueRedisTemplate,
      RqueueSystemConfigDao rqueueSystemConfigDao) {
    this.stringRqueueRedisTemplate = stringRqueueRedisTemplate;
    this.rqueueSystemConfigDao = rqueueSystemConfigDao;
  }

  private List<String> queueKeys(QueueConfig queueConfig) {
    List<String> keys =
        Arrays.asList(
            queueConfig.getName(),
            QueueUtils.getProcessingQueueName(queueConfig.getName()),
            QueueUtils.getQueueStatKey(queueConfig.getName()));
    if (queueConfig.isDelayed()) {
      keys.add(QueueUtils.getDelayedQueueName(queueConfig.getName()));
    }
    if (queueConfig.hasDeadLetterQueue()) {
      keys.addAll(queueConfig.getDeadLetterQueues());
    }
    return keys;
  }

  @Override
  public BaseResponse deleteQueue(String queueName) {
    QueueConfig queueConfig =
        rqueueSystemConfigDao.getQConfig(QueueUtils.getQueueConfigKey(queueName));
    BaseResponse baseResponse = new BaseResponse();
    if (queueConfig == null) {
      baseResponse.setCode(1);
      baseResponse.setMessage("Queue not found");
      return baseResponse;
    }
    queueConfig.setDeletedOn(System.currentTimeMillis());
    queueConfig.setDeleted(true);
    RedisUtils.executePipeLine(
        stringRqueueRedisTemplate.getRedisTemplate(),
        ((connection, keySerializer, valueSerializer) -> {
          for (String key : queueKeys(queueConfig)) {
            connection.del(key.getBytes());
          }
          connection.set(queueConfig.getId().getBytes(), valueSerializer.serialize(queueConfig));
        }));
    baseResponse.setCode(0);
    baseResponse.setMessage("Queue deleted");
    return baseResponse;
  }

  private QueueConfig createOrUpdateConfig(QueueConfig queueConfig, QueueDetail queueDetail) {
    String queueName = queueDetail.getQueueName();
    String qConfigId = QueueUtils.getQueueConfigKey(queueName);
    QueueConfig systemQueueConfig = queueConfig;
    boolean updated = false;
    boolean created = false;
    if (systemQueueConfig == null) {
      created = true;
      systemQueueConfig =
          new QueueConfig(
              qConfigId,
              queueName,
              queueDetail.getNumRetries(),
              queueDetail.isDelayedQueue(),
              queueDetail.getVisibilityTimeout());
    }
    if (queueDetail.isDlqSet()) {
      updated = systemQueueConfig.addDeadLetterQueue(queueDetail.getDeadLetterQueueName());
    }
    updated =
        systemQueueConfig.updateVisibilityTimeout(queueDetail.getVisibilityTimeout()) || updated;
    updated = systemQueueConfig.updateIsDelay(queueDetail.isDelayedQueue()) || updated;
    updated = systemQueueConfig.updateRetryCount(queueDetail.getNumRetries()) || updated;
    if (updated && !created) {
      systemQueueConfig.updateTime();
    }
    if (updated || created) {
      return systemQueueConfig;
    }
    return null;
  }

  private void createOrUpdateConfigs(
      Map<String, QueueDetail> queueDetailMap, Set<String> queueNames) {
    String[] queues = new String[queueNames.size()];
    int i = 0;
    for (String queue : queueNames) {
      queues[i++] = queue;
    }
    stringRqueueRedisTemplate.addToSet(QueueUtils.getQueuesKey(), queues);
    List<QueueConfig> queueConfigs = rqueueSystemConfigDao.findAllQConfig(Arrays.asList(queues));
    List<QueueConfig> newConfigs = new ArrayList<>();
    for (i = 0; i < queues.length; i++) {
      QueueDetail queueDetail = queueDetailMap.get(queues[i]);
      String queue = queues[i];
      QueueConfig dbConfig = null;
      for (QueueConfig queueConfig : queueConfigs) {
        if (queueConfig.getName().equals(queue)) {
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
  public void onApplicationEvent(QueueInitializationEvent event) {
    if (event.isStart()) {
      Map<String, QueueDetail> queueNameToQueueDetailMap = event.getQueueDetailMap();
      Set<String> queueNames = queueNameToQueueDetailMap.keySet();
      if (queueNames.isEmpty()) {
        return;
      }
      createOrUpdateConfigs(queueNameToQueueDetailMap, queueNames);
    }
  }

  @Override
  public List<String> getQueues() {
    Set<String> members = stringRqueueRedisTemplate.getMembers(QueueUtils.getQueuesKey());
    if (CollectionUtils.isEmpty(members)) {
      return Collections.emptyList();
    }
    return new ArrayList<>(members);
  }

  @Override
  public List<QueueConfig> getQueueConfigs(Collection<String> queues) {
    Collection<String> ids = new ArrayList<>();
    if (!CollectionUtils.isEmpty(queues)) {
      ids = queues.stream().map(QueueUtils::getQueueConfigKey).collect(Collectors.toList());
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
  public QueueConfig getQueueConfig(String queueName) {
    List<QueueConfig> queueConfigs = getQueueConfigs(Collections.singletonList(queueName));
    if (CollectionUtils.isEmpty(queueConfigs)) {
      return null;
    }
    return queueConfigs.get(0);
  }
}
