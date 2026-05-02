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

import com.github.sonus21.rqueue.models.registry.RqueueWorkerInfo;
import java.time.Duration;
import java.util.Collection;
import java.util.Map;

/**
 * Backend-shaped persistence primitives for the worker registry. The single
 * {@link RqueueWorkerRegistry} implementation in core orchestrates all heartbeat / view logic
 * and delegates storage to one of these stores. Redis and NATS provide concrete impls.
 *
 * <p>Operations are intentionally narrow and KV-shaped so this abstraction maps cleanly onto
 * both Redis (string keys + a hash per queue) and NATS JetStream KV (one bucket for worker
 * infos, another bucket for queue heartbeats keyed by {@code "<queue>.<workerId>"}).
 */
public interface WorkerRegistryStore {

  /**
   * Persist this worker's heartbeat record (host / pid / version / lastSeenAt). Writers SHOULD
   * apply the supplied TTL; backends without per-key TTL (e.g. NATS KV without per-message TTL)
   * may rely on a bucket-level {@code maxAge} configured to the same value.
   */
  void putWorkerInfo(String workerKey, RqueueWorkerInfo info, Duration ttl);

  /** Best-effort delete; called on graceful shutdown. */
  void deleteWorkerInfo(String workerKey);

  /**
   * Bulk-load worker infos by full key. Keys with no entry (or that fail to deserialize) are
   * omitted from the returned map. The map is keyed by {@link RqueueWorkerInfo} worker ID
   * ({@code getWorkerId()}).
   */
  Map<String, RqueueWorkerInfo> getWorkerInfos(Collection<String> workerKeys);

  /**
   * Write/overwrite this worker's heartbeat metadata for the given queue. Each (queueKey,
   * workerId) pair is independent — concurrent writes from different workers must not collide.
   */
  void putQueueHeartbeat(String queueKey, String workerId, String metadataJson);

  /**
   * Read all worker heartbeats currently registered for this queue, keyed by workerId. Empty
   * map if none. Implementations should not throw on a missing queue.
   */
  Map<String, String> getQueueHeartbeats(String queueKey);

  /** Remove specific worker heartbeats from a queue (lazy cleanup of stale entries). */
  void deleteQueueHeartbeats(String queueKey, String... workerIds);

  /**
   * Refresh the queue-heartbeat container's TTL. On Redis this is {@code EXPIRE} on the hash
   * key; on NATS KV the container TTL is the bucket's {@code maxAge} and individual writes
   * implicitly reset entry expiry, so impls may treat this as a no-op.
   */
  void refreshQueueTtl(String queueKey, Duration ttl);
}
