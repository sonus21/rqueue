/*
 * Copyright (c) 2026 Sonu Kumar
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

package com.github.sonus21.rqueue.redis.worker;

import com.github.sonus21.rqueue.common.RqueueRedisTemplate;
import com.github.sonus21.rqueue.config.RqueueConfig;
import com.github.sonus21.rqueue.models.registry.RqueueWorkerInfo;
import com.github.sonus21.rqueue.worker.WorkerRegistryStore;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.springframework.util.CollectionUtils;

/**
 * Redis-backed {@link WorkerRegistryStore}. Worker info lives at
 * {@code rqueueConfig.getWorkerRegistryKey(workerId)} as a serialized {@link RqueueWorkerInfo};
 * per-queue heartbeats live in a Redis hash at
 * {@code rqueueConfig.getWorkerRegistryQueueKey(queueName)} keyed by worker id with the JSON
 * metadata payload as the value.
 */
public class RedisWorkerRegistryStore implements WorkerRegistryStore {

  private final RqueueRedisTemplate<RqueueWorkerInfo> workerTemplate;
  private final RqueueRedisTemplate<String> stringTemplate;

  public RedisWorkerRegistryStore(RqueueConfig rqueueConfig) {
    this.workerTemplate = new RqueueRedisTemplate<>(rqueueConfig.getConnectionFactory());
    this.stringTemplate = new RqueueRedisTemplate<>(rqueueConfig.getConnectionFactory());
  }

  @Override
  public void putWorkerInfo(String workerKey, RqueueWorkerInfo info, Duration ttl) {
    workerTemplate.set(workerKey, info, ttl);
  }

  @Override
  public void deleteWorkerInfo(String workerKey) {
    workerTemplate.delete(workerKey);
  }

  @Override
  public Map<String, RqueueWorkerInfo> getWorkerInfos(Collection<String> workerKeys) {
    if (CollectionUtils.isEmpty(workerKeys)) {
      return Collections.emptyMap();
    }
    List<String> keys = new ArrayList<>(workerKeys);
    List<RqueueWorkerInfo> workerInfos = workerTemplate.mget(keys);
    if (CollectionUtils.isEmpty(workerInfos)) {
      return Collections.emptyMap();
    }
    Map<String, RqueueWorkerInfo> workerInfoById = new LinkedHashMap<>();
    for (RqueueWorkerInfo workerInfo : workerInfos) {
      if (workerInfo != null && workerInfo.getWorkerId() != null) {
        workerInfoById.put(workerInfo.getWorkerId(), workerInfo);
      }
    }
    return workerInfoById;
  }

  @Override
  public void putQueueHeartbeat(String queueKey, String workerId, String metadataJson) {
    stringTemplate.putHashValue(queueKey, workerId, metadataJson);
  }

  @Override
  public Map<String, String> getQueueHeartbeats(String queueKey) {
    Map<String, String> entries = stringTemplate.getHashEntries(queueKey);
    return entries == null ? Collections.emptyMap() : entries;
  }

  @Override
  public void deleteQueueHeartbeats(String queueKey, String... workerIds) {
    if (workerIds == null || workerIds.length == 0) {
      return;
    }
    stringTemplate.deleteHashValues(queueKey, workerIds);
  }

  @Override
  public void refreshQueueTtl(String queueKey, Duration ttl) {
    stringTemplate.expire(queueKey, ttl);
  }
}
