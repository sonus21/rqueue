/*
 * Copyright (c) 2020-2023 Sonu Kumar
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

package com.github.sonus21.rqueue.dao.impl;

import com.github.sonus21.rqueue.common.RqueueRedisTemplate;
import com.github.sonus21.rqueue.config.RqueueConfig;
import com.github.sonus21.rqueue.dao.RqueueSystemConfigDao;
import com.github.sonus21.rqueue.models.db.QueueConfig;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Repository;
import org.springframework.util.CollectionUtils;

@Repository
public class RqueueSystemConfigDaoImpl implements RqueueSystemConfigDao {

  private final RqueueRedisTemplate<QueueConfig> rqueueRedisTemplate;
  private final Map<String, QueueConfig> queueConfigMap = new ConcurrentHashMap<>();
  private final RqueueConfig rqueueConfig;

  @Autowired
  public RqueueSystemConfigDaoImpl(RqueueConfig rqueueConfig) {
    this(new RqueueRedisTemplate<>(rqueueConfig.getConnectionFactory()), rqueueConfig);
  }

  public RqueueSystemConfigDaoImpl(
      RqueueRedisTemplate<QueueConfig> rqueueRedisTemplate, RqueueConfig rqueueConfig) {
    this.rqueueRedisTemplate = rqueueRedisTemplate;
    this.rqueueConfig = rqueueConfig;
  }

  @Override
  public QueueConfig getConfigByName(String name) {
    return getConfigByName(name, false);
  }

  @Override
  public List<QueueConfig> getConfigByNames(Collection<String> names) {
    return findAllQConfig(
        names.stream().map(rqueueConfig::getQueueConfigKey).collect(Collectors.toList()));
  }

  @Override
  public QueueConfig getConfigByName(String name, boolean cached) {
    String queueConfigKey = rqueueConfig.getQueueConfigKey(name);
    return getQConfig(queueConfigKey, cached);
  }

  @Override
  public QueueConfig getQConfig(String id, boolean cached) {
    if (cached && queueConfigMap.containsKey(id)) {
      return queueConfigMap.get(id);
    }
    QueueConfig queueConfig = rqueueRedisTemplate.get(id);
    if (queueConfig != null) {
      queueConfigMap.put(id, queueConfig);
    }
    return queueConfig;
  }

  @Override
  public List<QueueConfig> findAllQConfig(Collection<String> ids) {
    return rqueueRedisTemplate.mget(ids).stream()
        .filter(Objects::nonNull)
        .collect(Collectors.toList());
  }

  @Override
  public void saveQConfig(QueueConfig queueConfig) {
    saveAllQConfig(Collections.singletonList(queueConfig));
  }

  @Override
  public void saveAllQConfig(List<QueueConfig> newConfigs) {
    if (!CollectionUtils.isEmpty(newConfigs)) {
      Map<String, QueueConfig> idToQueueConfig = new HashMap<>();
      for (QueueConfig queueConfig : newConfigs) {
        if (queueConfig == null) {
          throw new IllegalArgumentException("queueConfig cannot be null");
        }
        if (queueConfig.getId() == null) {
          throw new IllegalArgumentException("id cannot be null " + queueConfig);
        }
        idToQueueConfig.put(queueConfig.getId(), queueConfig);
      }
      for (String key : idToQueueConfig.keySet()) {
        queueConfigMap.remove(key);
      }
      rqueueRedisTemplate.mset(idToQueueConfig);
      for (String key : idToQueueConfig.keySet()) {
        queueConfigMap.remove(key);
      }
    }
  }

  @Override
  public void clearCacheByName(String name) {
    String key = rqueueConfig.getQueueConfigKey(name);
    queueConfigMap.remove(key);
  }
}
