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

package com.github.sonus21.rqueue.nats.worker;

import com.github.sonus21.rqueue.config.NatsBackendCondition;
import com.github.sonus21.rqueue.models.registry.RqueueWorkerInfo;
import com.github.sonus21.rqueue.worker.WorkerRegistryStore;
import io.nats.client.Connection;
import io.nats.client.JetStreamApiException;
import io.nats.client.KeyValue;
import io.nats.client.KeyValueManagement;
import io.nats.client.api.KeyValueConfiguration;
import io.nats.client.api.KeyValueEntry;
import io.nats.client.api.KeyValueStatus;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.springframework.context.annotation.Conditional;
import org.springframework.stereotype.Repository;

/**
 * NATS JetStream KV-backed {@link WorkerRegistryStore}. Uses two buckets so each can carry its
 * own bucket-level {@code maxAge}: {@code rqueue-workers} (worker info, TTL = workerTtl) and
 * {@code rqueue-worker-heartbeats} (per-(queue, workerId) heartbeats, TTL = queueTtl).
 *
 * <p>Per-queue heartbeats are stored as flattened keys of the form
 * {@code "<sanitizedQueueKey>__<sanitizedWorkerId>"}. Listing heartbeats for a queue iterates
 * all bucket keys with the matching {@code <sanitizedQueueKey>__} prefix.
 *
 * <p>Bucket {@code ttl} (NATS' name for entry max-age) is a one-shot configuration set at bucket
 * creation; {@link #refreshQueueTtl(String, Duration)} is therefore a no-op — every write into
 * the heartbeat bucket re-establishes the entry's age from zero, which is sufficient given the
 * registry rewrites heartbeats on the configured interval.
 *
 * <p>The first call lazily creates each bucket. {@code ttl} is fixed at bucket creation, so
 * existing buckets are reused even if the configured TTL has since changed.
 */
@Repository
@Conditional(NatsBackendCondition.class)
public class NatsWorkerRegistryStore implements WorkerRegistryStore {

  private static final Logger log = Logger.getLogger(NatsWorkerRegistryStore.class.getName());
  private static final String WORKER_BUCKET = "rqueue-workers";
  private static final String HEARTBEAT_BUCKET = "rqueue-worker-heartbeats";
  /** Separator used to flatten a {@code (queueKey, workerId)} pair into a single KV key. */
  private static final String SEP = "__";

  private final Connection connection;
  private final KeyValueManagement kvm;
  private final AtomicReference<KeyValue> workerKv = new AtomicReference<>();
  private final AtomicReference<KeyValue> heartbeatKv = new AtomicReference<>();
  /** Captured on first putWorkerInfo call so worker bucket gets the right maxAge. */
  private volatile Duration workerBucketTtl;

  /** Captured on first putQueueHeartbeat / refreshQueueTtl so heartbeat bucket gets the right maxAge. */
  private volatile Duration heartbeatBucketTtl;

  public NatsWorkerRegistryStore(Connection connection) throws IOException {
    this.connection = connection;
    this.kvm = connection.keyValueManagement();
  }

  @Override
  public void putWorkerInfo(String workerKey, RqueueWorkerInfo info, Duration ttl) {
    if (workerBucketTtl == null) {
      workerBucketTtl = ttl;
    }
    try {
      KeyValue kv = ensureBucket(workerKv, WORKER_BUCKET, workerBucketTtl);
      kv.put(sanitize(workerKey), serialize(info));
    } catch (IOException | JetStreamApiException e) {
      log.log(Level.WARNING, "putWorkerInfo " + workerKey + " failed", e);
    }
  }

  @Override
  public void deleteWorkerInfo(String workerKey) {
    try {
      KeyValue kv = ensureBucket(workerKv, WORKER_BUCKET, workerBucketTtl);
      kv.delete(sanitize(workerKey));
    } catch (IOException | JetStreamApiException e) {
      log.log(Level.WARNING, "deleteWorkerInfo " + workerKey + " failed", e);
    }
  }

  @Override
  public Map<String, RqueueWorkerInfo> getWorkerInfos(Collection<String> workerKeys) {
    if (workerKeys == null || workerKeys.isEmpty()) {
      return Collections.emptyMap();
    }
    Map<String, RqueueWorkerInfo> out = new LinkedHashMap<>();
    try {
      KeyValue kv = ensureBucket(workerKv, WORKER_BUCKET, workerBucketTtl);
      for (String key : workerKeys) {
        KeyValueEntry entry = kv.get(sanitize(key));
        if (entry == null || entry.getValue() == null) {
          continue;
        }
        RqueueWorkerInfo info = deserialize(entry.getValue());
        if (info != null && info.getWorkerId() != null) {
          out.put(info.getWorkerId(), info);
        }
      }
    } catch (IOException | JetStreamApiException e) {
      log.log(Level.WARNING, "getWorkerInfos failed", e);
    }
    return out;
  }

  @Override
  public void putQueueHeartbeat(String queueKey, String workerId, String metadataJson) {
    if (heartbeatBucketTtl == null) {
      // Best-effort default — overwritten by refreshQueueTtl when the registry computes the real
      // value.
      heartbeatBucketTtl = Duration.ofHours(1);
    }
    try {
      KeyValue kv = ensureBucket(heartbeatKv, HEARTBEAT_BUCKET, heartbeatBucketTtl);
      kv.put(compositeKey(queueKey, workerId), metadataJson.getBytes());
    } catch (IOException | JetStreamApiException e) {
      log.log(Level.WARNING, "putQueueHeartbeat queue=" + queueKey + " failed", e);
    }
  }

  @Override
  public Map<String, String> getQueueHeartbeats(String queueKey) {
    Map<String, String> out = new LinkedHashMap<>();
    try {
      KeyValue kv = ensureBucket(heartbeatKv, HEARTBEAT_BUCKET, heartbeatBucketTtl);
      String prefix = sanitize(queueKey) + SEP;
      List<String> keys = new ArrayList<>(kv.keys());
      for (String k : keys) {
        if (!k.startsWith(prefix)) {
          continue;
        }
        KeyValueEntry entry = kv.get(k);
        if (entry == null || entry.getValue() == null) {
          continue;
        }
        String workerId = k.substring(prefix.length());
        out.put(workerId, new String(entry.getValue()));
      }
    } catch (IOException | JetStreamApiException | InterruptedException e) {
      if (e instanceof InterruptedException) {
        Thread.currentThread().interrupt();
      }
      log.log(Level.WARNING, "getQueueHeartbeats queue=" + queueKey + " failed", e);
    }
    return out;
  }

  @Override
  public void deleteQueueHeartbeats(String queueKey, String... workerIds) {
    if (workerIds == null || workerIds.length == 0) {
      return;
    }
    try {
      KeyValue kv = ensureBucket(heartbeatKv, HEARTBEAT_BUCKET, heartbeatBucketTtl);
      for (String workerId : workerIds) {
        kv.delete(compositeKey(queueKey, workerId));
      }
    } catch (IOException | JetStreamApiException e) {
      log.log(Level.WARNING, "deleteQueueHeartbeats queue=" + queueKey + " failed", e);
    }
  }

  @Override
  public void refreshQueueTtl(String queueKey, Duration ttl) {
    // NATS KV applies maxAge at the bucket level and resets per-entry age on each write.
    // The registry already rewrites heartbeats on its configured interval, so each fresh put
    // implicitly resets expiry. We only capture the first observed ttl so the bucket created
    // on first use carries the correct maxAge.
    if (heartbeatBucketTtl == null) {
      heartbeatBucketTtl = ttl;
    }
  }

  // ---- helpers ----------------------------------------------------------

  private KeyValue ensureBucket(AtomicReference<KeyValue> ref, String bucketName, Duration maxAge)
      throws IOException, JetStreamApiException {
    KeyValue cached = ref.get();
    if (cached != null) {
      return cached;
    }
    synchronized (ref) {
      cached = ref.get();
      if (cached != null) {
        return cached;
      }
      try {
        KeyValueStatus status = kvm.getStatus(bucketName);
        if (status != null) {
          KeyValue kv = connection.keyValue(bucketName);
          ref.set(kv);
          return kv;
        }
      } catch (JetStreamApiException missing) {
        // fall through to create
      }
      KeyValueConfiguration.Builder cfg = KeyValueConfiguration.builder().name(bucketName);
      if (maxAge != null && !maxAge.isZero() && !maxAge.isNegative()) {
        cfg.ttl(maxAge);
      }
      kvm.create(cfg.build());
      KeyValue kv = connection.keyValue(bucketName);
      ref.set(kv);
      return kv;
    }
  }

  private static String compositeKey(String queueKey, String workerId) {
    return sanitize(queueKey) + SEP + sanitize(workerId);
  }

  /** KV keys allow {@code [A-Za-z0-9_=.-]} only. */
  private static String sanitize(String key) {
    return key == null ? "_" : key.replaceAll("[^A-Za-z0-9_=.-]", "_");
  }

  private static byte[] serialize(RqueueWorkerInfo info) throws IOException {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    try (ObjectOutputStream oos = new ObjectOutputStream(baos)) {
      oos.writeObject(info);
    }
    return baos.toByteArray();
  }

  private static RqueueWorkerInfo deserialize(byte[] bytes) {
    try (ObjectInputStream ois = new ObjectInputStream(new ByteArrayInputStream(bytes))) {
      Object o = ois.readObject();
      return o instanceof RqueueWorkerInfo ? (RqueueWorkerInfo) o : null;
    } catch (IOException | ClassNotFoundException e) {
      log.log(Level.WARNING, "deserialize RqueueWorkerInfo failed", e);
      return null;
    }
  }
}
