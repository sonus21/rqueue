/*
 * Copyright (c) 2024-2026 Sonu Kumar
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * You may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 */
package com.github.sonus21.rqueue.nats.dao;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

import com.github.sonus21.rqueue.models.db.QueueConfig;
import com.github.sonus21.rqueue.nats.AbstractJetStreamIT;
import com.github.sonus21.rqueue.nats.RqueueNatsConfig;
import com.github.sonus21.rqueue.serdes.RqJacksonSerDes;
import com.github.sonus21.rqueue.nats.internal.NatsProvisioner;
import com.github.sonus21.rqueue.serdes.SerializationUtils;
import io.nats.client.JetStreamApiException;
import io.nats.client.KeyValueManagement;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** Round-trip exercise for {@link NatsRqueueSystemConfigDao} against a real JetStream KV. */
class NatsRqueueSystemConfigDaoIT extends AbstractJetStreamIT {

  private NatsRqueueSystemConfigDao dao;

  @BeforeEach
  void freshBucket() throws IOException, JetStreamApiException {
    KeyValueManagement kvm = connection.keyValueManagement();
    try {
      kvm.delete("rqueue-queue-config");
    } catch (JetStreamApiException notFound) {
      // first run
    }
    NatsProvisioner provisioner = new NatsProvisioner(
        connection, connection.jetStreamManagement(), RqueueNatsConfig.defaults());
    dao = new NatsRqueueSystemConfigDao(provisioner, new RqJacksonSerDes(SerializationUtils.getObjectMapper()));
  }

  private QueueConfig sample(String id, String name) {
    QueueConfig c = new QueueConfig();
    c.setId(id);
    c.setName(name);
    c.setQueueName(name);
    c.setNumRetry(3);
    c.setVisibilityTimeout(30_000L);
    return c;
  }

  @Test
  void saveAndGetByName() {
    QueueConfig c = sample("id-1", "orders");
    dao.saveQConfig(c);

    QueueConfig back = dao.getConfigByName("orders", false);
    assertNotNull(back);
    assertEquals("id-1", back.getId());
    assertEquals("orders", back.getName());
    assertEquals(3, back.getNumRetry());
    assertEquals(30_000L, back.getVisibilityTimeout());
  }

  @Test
  void getByNameMissingReturnsNull() {
    assertNull(dao.getConfigByName("never-saved", false));
  }

  @Test
  void cachedReadDoesNotRoundtripJetStream() {
    QueueConfig c = sample("id-2", "cached-q");
    dao.saveQConfig(c);
    // First read primes the cache; second hits cache.
    QueueConfig first = dao.getConfigByName("cached-q");
    QueueConfig second = dao.getConfigByName("cached-q");
    assertNotNull(first);
    assertEquals(first, second);

    dao.clearCacheByName("cached-q");
    QueueConfig third = dao.getConfigByName("cached-q");
    assertNotNull(third);
    assertEquals("cached-q", third.getName());
  }

  @Test
  void saveAllAndGetByNames() {
    dao.saveAllQConfig(Arrays.asList(sample("a", "qa"), sample("b", "qb"), sample("c", "qc")));
    List<QueueConfig> got = dao.getConfigByNames(Arrays.asList("qa", "qb", "missing"));
    assertEquals(2, got.size());
  }

  @Test
  void getQConfigByIdScan() {
    dao.saveAllQConfig(Arrays.asList(sample("alpha", "q1"), sample("beta", "q2")));
    QueueConfig found = dao.getQConfig("beta", false);
    assertNotNull(found);
    assertEquals("q2", found.getName());
  }

  @Test
  void sanitizationLetsNamesWithIllegalCharsRoundTrip() {
    // Note: name is preserved on the QueueConfig itself; only the KV key is sanitized.
    QueueConfig c = sample("id-x", "queue#with$weird:chars");
    dao.saveQConfig(c);
    QueueConfig back = dao.getConfigByName("queue#with$weird:chars", false);
    assertNotNull(back);
    assertEquals("id-x", back.getId());
  }
}
