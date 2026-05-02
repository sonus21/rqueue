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

package com.github.sonus21.rqueue.worker;

import com.github.sonus21.rqueue.config.RqueueConfig;
import com.github.sonus21.rqueue.core.EndpointRegistry;
import com.github.sonus21.rqueue.listener.QueueDetail;
import com.github.sonus21.rqueue.models.event.RqueueBootstrapEvent;
import com.github.sonus21.rqueue.models.registry.RqueueWorkerInfo;
import com.github.sonus21.rqueue.models.registry.RqueueWorkerPollerMetadata;
import com.github.sonus21.rqueue.models.registry.RqueueWorkerPollerView;
import com.github.sonus21.rqueue.serdes.RqueueSerDes;
import com.github.sonus21.rqueue.serdes.SerializationUtils;
import com.github.sonus21.rqueue.utils.DateTimeUtils;
import com.github.sonus21.rqueue.utils.QueueThreadPool;
import com.github.sonus21.rqueue.utils.StringUtils;
import java.lang.management.ManagementFactory;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.ApplicationListener;
import org.springframework.util.CollectionUtils;

/**
 * Backend-agnostic worker registry. All heartbeat scheduling, in-memory bookkeeping, and view
 * assembly lives here; storage is delegated to a {@link WorkerRegistryStore}, of which Redis
 * and NATS JetStream KV provide concrete implementations.
 */
@Slf4j
public class RqueueWorkerRegistryImpl
    implements RqueueWorkerRegistry, ApplicationListener<RqueueBootstrapEvent> {
  private final RqueueSerDes serDes = SerializationUtils.getSerDes();
  private final RqueueConfig rqueueConfig;
  private final WorkerRegistryStore store;
  private final String workerId;
  private final String host;
  private final String pid;
  private final long startedAt;
  private final Map<String, Long> lastMessageAtByQueue = new ConcurrentHashMap<>();
  private final Map<String, Long> lastPollAtByQueue = new ConcurrentHashMap<>();
  private final Map<String, Long> lastQueueHeartbeatAt = new ConcurrentHashMap<>();
  private final Map<String, Long> lastQueueTtlRefreshAt = new ConcurrentHashMap<>();
  private final Map<String, Long> lastCapacityExhaustedAtByQueue = new ConcurrentHashMap<>();
  private final Map<String, Long> capacityExhaustedCountByQueue = new ConcurrentHashMap<>();
  private volatile long lastWorkerHeartbeatAt;

  public RqueueWorkerRegistryImpl(RqueueConfig rqueueConfig, WorkerRegistryStore store) {
    this.rqueueConfig = rqueueConfig;
    this.store = store;
    this.workerId = RqueueConfig.getBrokerId();
    this.host = getHostName();
    this.pid = getPid();
    this.startedAt = System.currentTimeMillis();
  }

  @Override
  public void recordQueuePoll(
      QueueDetail queueDetail, QueueThreadPool queueThreadPool, boolean messageReceived) {
    if (!rqueueConfig.isWorkerRegistryEnabled()) {
      return;
    }
    String registryQueueName = registryQueueName(queueDetail);
    long now = System.currentTimeMillis();
    if (messageReceived) {
      lastMessageAtByQueue.put(registryQueueName, now);
    }
    lastPollAtByQueue.put(registryQueueName, now);
    refreshWorkerInfoIfRequired(now);
    if (!queueHeartbeatRequired(registryQueueName, now)) {
      return;
    }
    publishHeartbeat(registryQueueName, queueThreadPool, now);
  }

  @Override
  public void recordQueueCapacityExhausted(
      QueueDetail queueDetail, QueueThreadPool queueThreadPool) {
    if (!rqueueConfig.isWorkerRegistryEnabled()) {
      return;
    }
    String registryQueueName = registryQueueName(queueDetail);
    long now = System.currentTimeMillis();
    refreshWorkerInfoIfRequired(now);
    lastCapacityExhaustedAtByQueue.put(registryQueueName, now);
    capacityExhaustedCountByQueue.compute(registryQueueName, (key, count) -> {
      if (count == null) {
        return 1L;
      }
      if (count == Long.MAX_VALUE) {
        return Long.MAX_VALUE;
      }
      return count + 1L;
    });
    if (!queueHeartbeatRequired(registryQueueName, now)) {
      return;
    }
    publishHeartbeat(registryQueueName, queueThreadPool, now);
  }

  private void publishHeartbeat(
      String registryQueueName, QueueThreadPool queueThreadPool, long now) {
    RqueueWorkerPollerMetadata metadata = buildMetadata(registryQueueName, queueThreadPool);
    try {
      String queueKey = rqueueConfig.getWorkerRegistryQueueKey(registryQueueName);
      store.putQueueHeartbeat(queueKey, workerId, serDes.serializeAsString(metadata));
      refreshQueueTtlIfRequired(registryQueueName, now);
      lastQueueHeartbeatAt.put(registryQueueName, now);
    } catch (Exception e) {
      log.warn("Worker registry serialization failed for queue {}", registryQueueName, e);
    }
  }

  @Override
  public List<RqueueWorkerPollerView> getQueueWorkers(String queueName) {
    if (!rqueueConfig.isWorkerRegistryEnabled()) {
      return Collections.emptyList();
    }
    String queueKey = rqueueConfig.getWorkerRegistryQueueKey(queueName);
    Map<String, String> rawEntries = store.getQueueHeartbeats(queueKey);
    if (CollectionUtils.isEmpty(rawEntries)) {
      return Collections.emptyList();
    }
    long now = System.currentTimeMillis();
    long staleAfter = 2 * rqueueConfig.getWorkerRegistryQueueHeartbeatInterval().toMillis();
    Map<String, RqueueWorkerPollerMetadata> metadataByWorkerId = new LinkedHashMap<>();
    List<String> toDelete = new ArrayList<>();
    for (Map.Entry<String, String> entry : rawEntries.entrySet()) {
      try {
        RqueueWorkerPollerMetadata metadata =
            serDes.deserialize(entry.getValue(), RqueueWorkerPollerMetadata.class);
        if (metadata == null || metadata.getWorkerId() == null) {
          toDelete.add(entry.getKey());
          continue;
        }
        // Lazy cleanup for entries that are far older than the queue retention window.
        if (now - metadata.getLastPollAt()
            > rqueueConfig.getWorkerRegistryQueueTtl().toMillis()) {
          toDelete.add(entry.getKey());
          continue;
        }
        metadataByWorkerId.put(entry.getKey(), metadata);
      } catch (Exception e) {
        log.warn("Worker registry deserialization failed for queue {}", queueName, e);
        toDelete.add(entry.getKey());
      }
    }
    if (!toDelete.isEmpty()) {
      store.deleteQueueHeartbeats(queueKey, toDelete.toArray(new String[0]));
    }
    if (metadataByWorkerId.isEmpty()) {
      return Collections.emptyList();
    }
    Map<String, RqueueWorkerInfo> workerInfoById = loadWorkerInfo(metadataByWorkerId.keySet());
    List<RqueueWorkerPollerView> rows = new ArrayList<>();
    for (Map.Entry<String, RqueueWorkerPollerMetadata> entry : metadataByWorkerId.entrySet()) {
      String workerId = entry.getKey();
      RqueueWorkerPollerMetadata metadata = entry.getValue();
      RqueueWorkerInfo workerInfo = workerInfoById.get(workerId);
      long lastActivityAt = Math.max(
          metadata.getLastPollAt(),
          metadata.getLastCapacityExhaustedAt() == null
              ? 0
              : metadata.getLastCapacityExhaustedAt());
      boolean stale = now - lastActivityAt > staleAfter || workerInfo == null;
      rows.add(RqueueWorkerPollerView.builder()
          .queue(queueName)
          .workerId(workerId)
          .host(workerInfo == null ? "unknown" : workerInfo.getHost())
          .pid(workerInfo == null ? "" : workerInfo.getPid())
          .status(stale ? "STALE" : "ACTIVE")
          .lastPollAt(metadata.getLastPollAt())
          .lastPollAge(
              formatAge(now, metadata.getLastPollAt() == 0 ? null : metadata.getLastPollAt()))
          .lastMessageAt(metadata.getLastMessageAt())
          .lastMessageAge(formatAge(now, metadata.getLastMessageAt()))
          .lastCapacityExhaustedAt(metadata.getLastCapacityExhaustedAt())
          .lastCapacityExhaustedAge(formatAge(now, metadata.getLastCapacityExhaustedAt()))
          .capacityExhaustedCount(metadata.getCapacityExhaustedCount())
          .build());
    }
    rows.sort(Comparator.comparingLong(RqueueWorkerPollerView::getLastPollAt).reversed());
    return rows;
  }

  @Override
  public void onApplicationEvent(RqueueBootstrapEvent event) {
    if (!rqueueConfig.isWorkerRegistryEnabled()) {
      return;
    }
    if (event.isStartup()) {
      refreshWorkerInfo(System.currentTimeMillis());
    } else if (event.isShutdown()) {
      cleanup();
    }
  }

  private void refreshWorkerInfoIfRequired(long now) {
    if (now - lastWorkerHeartbeatAt
        < rqueueConfig.getWorkerRegistryWorkerHeartbeatInterval().toMillis()) {
      return;
    }
    refreshWorkerInfo(now);
  }

  private void refreshWorkerInfo(long now) {
    RqueueWorkerInfo workerInfo = RqueueWorkerInfo.builder()
        .workerId(workerId)
        .host(host)
        .pid(pid)
        .version(rqueueConfig.getLibVersion())
        .startedAt(startedAt)
        .lastSeenAt(now)
        .build();
    store.putWorkerInfo(
        rqueueConfig.getWorkerRegistryKey(workerId),
        workerInfo,
        rqueueConfig.getWorkerRegistryWorkerTtl());
    lastWorkerHeartbeatAt = now;
  }

  private boolean queueHeartbeatRequired(String queueName, long now) {
    Long lastHeartbeat = lastQueueHeartbeatAt.get(queueName);
    if (lastHeartbeat == null) {
      return true;
    }
    return now - lastHeartbeat
        >= rqueueConfig.getWorkerRegistryQueueHeartbeatInterval().toMillis();
  }

  private void refreshQueueTtlIfRequired(String queueName, long now) {
    Duration ttl = rqueueConfig.getWorkerRegistryQueueTtl();
    long refreshIntervalInMillis = Math.max(1000L, ttl.toMillis() / 2);
    Long lastRefreshAt = lastQueueTtlRefreshAt.get(queueName);
    if (lastRefreshAt != null && now - lastRefreshAt < refreshIntervalInMillis) {
      return;
    }
    store.refreshQueueTtl(rqueueConfig.getWorkerRegistryQueueKey(queueName), ttl);
    lastQueueTtlRefreshAt.put(queueName, now);
  }

  private void cleanup() {
    store.deleteWorkerInfo(rqueueConfig.getWorkerRegistryKey(workerId));
    for (QueueDetail queueDetail : EndpointRegistry.getActiveQueueDetails()) {
      store.deleteQueueHeartbeats(
          rqueueConfig.getWorkerRegistryQueueKey(registryQueueName(queueDetail)), workerId);
    }
    lastMessageAtByQueue.clear();
    lastPollAtByQueue.clear();
    lastQueueHeartbeatAt.clear();
    lastQueueTtlRefreshAt.clear();
    lastCapacityExhaustedAtByQueue.clear();
    capacityExhaustedCountByQueue.clear();
    lastWorkerHeartbeatAt = 0L;
  }

  private RqueueWorkerPollerMetadata buildMetadata(
      String registryQueueName, QueueThreadPool queueThreadPool) {
    return RqueueWorkerPollerMetadata.builder()
        .workerId(workerId)
        .lastPollAt(lastPollAtByQueue.getOrDefault(registryQueueName, 0L))
        .lastMessageAt(lastMessageAtByQueue.get(registryQueueName))
        .lastCapacityExhaustedAt(lastCapacityExhaustedAtByQueue.get(registryQueueName))
        .capacityExhaustedCount(capacityExhaustedCountByQueue.getOrDefault(registryQueueName, 0L))
        .build();
  }

  private Map<String, RqueueWorkerInfo> loadWorkerInfo(Collection<String> workerIds) {
    List<String> keys = new ArrayList<>(workerIds.size());
    for (String workerId : workerIds) {
      keys.add(rqueueConfig.getWorkerRegistryKey(workerId));
    }
    Map<String, RqueueWorkerInfo> result = store.getWorkerInfos(keys);
    return result == null ? Collections.emptyMap() : result;
  }

  private static String formatAge(long now, Long time) {
    if (time == null || time == 0) {
      return "";
    }
    return DateTimeUtils.milliToHumanRepresentation(now - time);
  }

  private static String registryQueueName(QueueDetail queueDetail) {
    if (queueDetail.isSystemGenerated() && !StringUtils.isEmpty(queueDetail.getPriorityGroup())) {
      return queueDetail.getPriorityGroup();
    }
    return queueDetail.getName();
  }

  private static String getPid() {
    String runtimeName = ManagementFactory.getRuntimeMXBean().getName();
    int index = runtimeName.indexOf('@');
    if (index == -1) {
      return runtimeName;
    }
    return runtimeName.substring(0, index);
  }

  private static String getHostName() {
    try {
      return InetAddress.getLocalHost().getHostName();
    } catch (UnknownHostException e) {
      return "unknown";
    }
  }
}
