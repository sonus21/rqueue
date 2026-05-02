/*
 * Copyright (c) 2024-2026 Sonu Kumar
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
 */

package com.github.sonus21.rqueue.nats.dao;

import com.github.sonus21.rqueue.config.NatsBackendCondition;
import com.github.sonus21.rqueue.dao.RqueueQStatsDao;
import com.github.sonus21.rqueue.models.db.QueueStatistics;
import com.github.sonus21.rqueue.nats.internal.NatsProvisioner;
import com.github.sonus21.rqueue.nats.kv.NatsKvBuckets;
import io.nats.client.JetStreamApiException;
import io.nats.client.KeyValue;
import io.nats.client.api.KeyValueEntry;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.springframework.context.annotation.Conditional;
import org.springframework.context.annotation.DependsOn;
import org.springframework.stereotype.Repository;

/**
 * NATS-backed {@link RqueueQStatsDao} using the {@code rqueue-queue-stats} JetStream KV bucket.
 *
 * <p>Each {@link QueueStatistics} entry is stored as a single KV record keyed by its
 * {@link QueueStatistics#getId()} (e.g. {@code __rq::q-stat::job-morgue}), serialized via Java
 * serialization to match the other NATS DAO implementations. The key is sanitized to the NATS KV
 * character set before storage.
 *
 * <p>No bucket-level TTL is set: the aggregator service calls
 * {@link QueueStatistics#pruneStats} before each {@link #save}, so stale per-day entries inside
 * the object are trimmed to {@code rqueue.web.statistic.history.day} days automatically.
 */
@Repository
@Conditional(NatsBackendCondition.class)
@DependsOn("natsKvBucketValidator")
public class NatsRqueueQStatsDao implements RqueueQStatsDao {

  private static final Logger log = Logger.getLogger(NatsRqueueQStatsDao.class.getName());
  private static final String BUCKET_NAME = NatsKvBuckets.QUEUE_STATS;

  private final NatsProvisioner provisioner;
  private final com.github.sonus21.rqueue.serdes.RqueueSerDes serdes;

  public NatsRqueueQStatsDao(NatsProvisioner provisioner, com.github.sonus21.rqueue.serdes.RqueueSerDes serdes) {
    this.provisioner = provisioner;
    this.serdes = serdes;
  }

  private KeyValue kv() throws IOException, JetStreamApiException {
    return provisioner.ensureKv(BUCKET_NAME, null);
  }

  @Override
  public QueueStatistics findById(String id) {
    if (id == null) {
      return null;
    }
    try {
      KeyValueEntry entry = kv().get(sanitize(id));
      if (entry == null || entry.getValue() == null) {
        return null;
      }
      return deserialize(entry.getValue());
    } catch (IOException | JetStreamApiException e) {
      log.log(Level.WARNING, "findById id=" + id + " failed", e);
      return null;
    }
  }

  @Override
  public List<QueueStatistics> findAll(Collection<String> ids) {
    List<QueueStatistics> out = new ArrayList<>(ids.size());
    for (String id : ids) {
      QueueStatistics stat = findById(id);
      if (stat != null) {
        out.add(stat);
      }
    }
    return out;
  }

  @Override
  public void save(QueueStatistics queueStatistics) {
    if (queueStatistics == null) {
      throw new IllegalArgumentException("queueStatistics cannot be null");
    }
    if (queueStatistics.getId() == null) {
      throw new IllegalArgumentException("id cannot be null: " + queueStatistics);
    }
    try {
      kv().put(sanitize(queueStatistics.getId()), serialize(queueStatistics));
    } catch (IOException | JetStreamApiException e) {
      log.log(Level.WARNING, "save id=" + queueStatistics.getId() + " failed", e);
    }
  }

  // ---- helpers ----------------------------------------------------------

  private byte[] serialize(QueueStatistics stat) throws IOException {
    return serdes.serialize(stat);
  }

  private QueueStatistics deserialize(byte[] bytes) {
    try {
      return serdes.deserialize(bytes, QueueStatistics.class);
    } catch (Exception e) {
      log.log(Level.WARNING, "deserialize QueueStatistics failed", e);
      return null;
    }
  }

  /** KV keys allow {@code [A-Za-z0-9_=.-]} only. */
  private static String sanitize(String key) {
    return key == null ? "_" : key.replaceAll("[^A-Za-z0-9_=.-]", "_");
  }
}
