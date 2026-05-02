/*
 * Copyright (c) 2026 Sonu Kumar
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
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.github.sonus21.rqueue.models.db.RqueueJob;
import com.github.sonus21.rqueue.models.enums.JobStatus;
import com.github.sonus21.rqueue.nats.AbstractJetStreamIT;
import com.github.sonus21.rqueue.nats.RqueueNatsConfig;
import com.github.sonus21.rqueue.nats.internal.NatsProvisioner;
import com.github.sonus21.rqueue.serdes.RqJacksonSerDes;
import com.github.sonus21.rqueue.serdes.SerializationUtils;
import io.nats.client.JetStreamApiException;
import io.nats.client.KeyValueManagement;
import java.io.IOException;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** Round-trip exercise for {@link NatsRqueueJobDao} against a real JetStream KV. */
class NatsRqueueJobDaoIT extends AbstractJetStreamIT {

  private NatsRqueueJobDao dao;

  @BeforeEach
  void freshBucket() throws IOException, JetStreamApiException {
    KeyValueManagement kvm = connection.keyValueManagement();
    try {
      kvm.delete("rqueue-jobs");
    } catch (JetStreamApiException notFound) {
      // first run
    }
    NatsProvisioner provisioner = new NatsProvisioner(
        connection, connection.jetStreamManagement(), RqueueNatsConfig.defaults());
    dao = new NatsRqueueJobDao(
        provisioner, new RqJacksonSerDes(SerializationUtils.getObjectMapper()));
  }

  private RqueueJob job(String id, String messageId) {
    RqueueJob j = new RqueueJob();
    j.setId(id);
    j.setMessageId(messageId);
    j.setStatus(JobStatus.CREATED);
    j.setCreatedAt(System.currentTimeMillis());
    return j;
  }

  @Test
  void saveAndFindById() {
    RqueueJob j = job("j1", "m1");
    dao.save(j, Duration.ofMinutes(1));

    RqueueJob back = dao.findById("j1");
    assertNotNull(back);
    assertEquals("j1", back.getId());
    assertEquals("m1", back.getMessageId());
    assertEquals(JobStatus.CREATED, back.getStatus());
  }

  @Test
  void findByIdMissingReturnsNull() {
    assertNull(dao.findById("never-saved"));
  }

  @Test
  void findJobsByIdIn() {
    dao.save(job("a", "ma"), Duration.ofMinutes(1));
    dao.save(job("b", "mb"), Duration.ofMinutes(1));
    dao.save(job("c", "mc"), Duration.ofMinutes(1));
    List<RqueueJob> got = dao.findJobsByIdIn(Arrays.asList("a", "b", "missing"));
    assertEquals(2, got.size());
  }

  @Test
  void findByMessageId() {
    dao.save(job("j1", "msg-X"), Duration.ofMinutes(1));
    dao.save(job("j2", "msg-X"), Duration.ofMinutes(1)); // same message, different attempt
    dao.save(job("j3", "msg-Y"), Duration.ofMinutes(1));
    List<RqueueJob> jobsForX = dao.finByMessageId("msg-X");
    assertEquals(2, jobsForX.size());
    assertTrue(jobsForX.stream().allMatch(j -> "msg-X".equals(j.getMessageId())));
  }

  @Test
  void findByMessageIdIn() {
    dao.save(job("j1", "m1"), Duration.ofMinutes(1));
    dao.save(job("j2", "m2"), Duration.ofMinutes(1));
    dao.save(job("j3", "m3"), Duration.ofMinutes(1));
    List<RqueueJob> got = dao.finByMessageIdIn(Arrays.asList("m1", "m3"));
    assertEquals(2, got.size());
  }

  @Test
  void deleteRemovesEntry() {
    dao.save(job("to-delete", "mx"), Duration.ofMinutes(1));
    assertNotNull(dao.findById("to-delete"));
    dao.delete("to-delete");
    assertNull(dao.findById("to-delete"));
  }

  @Test
  void sanitizationLetsIllegalIdsRoundTrip() {
    RqueueJob j = job("job#with$weird:chars", "msg-1");
    dao.save(j, Duration.ofMinutes(1));
    RqueueJob back = dao.findById("job#with$weird:chars");
    assertNotNull(back);
    assertEquals("msg-1", back.getMessageId());
  }
}
