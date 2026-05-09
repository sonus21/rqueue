/*
 * Copyright (c) 2026 Sonu Kumar
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * You may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 */
package com.github.sonus21.rqueue.nats.service;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.github.sonus21.rqueue.core.RqueueMessage;
import com.github.sonus21.rqueue.core.support.RqueueMessageUtils;
import com.github.sonus21.rqueue.models.db.MessageMetadata;
import com.github.sonus21.rqueue.models.enums.MessageStatus;
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
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** Round-trip exercise for {@link NatsRqueueMessageMetadataService} against a real JetStream KV. */
class NatsRqueueMessageMetadataServiceIT extends AbstractJetStreamIT {

  private NatsRqueueMessageMetadataService svc;

  @BeforeEach
  void freshBucket() throws IOException, JetStreamApiException {
    KeyValueManagement kvm = connection.keyValueManagement();
    try {
      kvm.delete("rqueue-message-metadata");
    } catch (JetStreamApiException notFound) {
      // first run
    }
    NatsProvisioner provisioner = new NatsProvisioner(
        connection, connection.jetStreamManagement(), RqueueNatsConfig.defaults());
    svc = new NatsRqueueMessageMetadataService(
        provisioner, new RqJacksonSerDes(SerializationUtils.getObjectMapper()));
  }

  private RqueueMessage rqueueMessage(String queue, String id) {
    return RqueueMessage.builder().id(id).queueName(queue).message("payload").build();
  }

  private MessageMetadata metadata(String queue, String id, MessageStatus status) {
    MessageMetadata m = new MessageMetadata(rqueueMessage(queue, id), status);
    m.setUpdatedOn(System.currentTimeMillis());
    return m;
  }

  @Test
  void saveAndGetById() {
    MessageMetadata m = metadata("orders", "msg-1", MessageStatus.ENQUEUED);
    svc.save(m, Duration.ofMinutes(1), false);
    MessageMetadata back = svc.get(m.getId());
    assertNotNull(back);
    assertEquals(MessageStatus.ENQUEUED, back.getStatus());
  }

  @Test
  void getByMessageIdLooksUpComputedMetaId() {
    MessageMetadata m = metadata("orders", "msg-2", MessageStatus.ENQUEUED);
    svc.save(m, Duration.ofMinutes(1), false);
    MessageMetadata back = svc.getByMessageId("orders", "msg-2");
    assertNotNull(back);
    assertEquals(RqueueMessageUtils.getMessageMetaId("orders", "msg-2"), back.getId());
  }

  @Test
  void deleteRemovesEntry() {
    MessageMetadata m = metadata("orders", "msg-3", MessageStatus.ENQUEUED);
    svc.save(m, Duration.ofMinutes(1), false);
    svc.delete(m.getId());
    assertNull(svc.get(m.getId()));
  }

  @Test
  void deleteMessageMarksDeletedFlag() {
    MessageMetadata m = metadata("orders", "msg-4", MessageStatus.ENQUEUED);
    svc.save(m, Duration.ofMinutes(1), false);
    assertTrue(svc.deleteMessage("orders", "msg-4", Duration.ofMinutes(1)));
    MessageMetadata back = svc.getByMessageId("orders", "msg-4");
    assertNotNull(back);
    assertTrue(back.isDeleted());
    assertNotNull(back.getDeletedOn());
  }

  @Test
  void deleteMessageOnMissingCreatesTombstone() {
    // NATS doesn't store metadata at enqueue time for stream-resident messages, so a
    // dashboard delete request can land before any metadata exists. The contract (shared
    // with the Redis impl) is to write a tombstone keyed by metaId so subsequent peeks
    // render the row as deleted, and return true.
    assertTrue(svc.deleteMessage("orders", "never-saved", Duration.ofMinutes(1)));
    MessageMetadata tombstone = svc.getByMessageId("orders", "never-saved");
    assertNotNull(tombstone);
    assertTrue(tombstone.isDeleted());
    assertEquals(MessageStatus.DELETED, tombstone.getStatus());
  }

  @Test
  void getOrCreateReturnsExistingWhenPresent() {
    MessageMetadata seeded = metadata("orders", "msg-5", MessageStatus.ENQUEUED);
    svc.save(seeded, Duration.ofMinutes(1), false);
    MessageMetadata got = svc.getOrCreateMessageMetadata(rqueueMessage("orders", "msg-5"));
    assertEquals(seeded.getId(), got.getId());
  }

  @Test
  void getOrCreateReturnsNewWhenAbsent() {
    MessageMetadata got = svc.getOrCreateMessageMetadata(rqueueMessage("orders", "fresh"));
    assertNotNull(got);
    assertEquals(MessageStatus.ENQUEUED, got.getStatus());
  }

  @Test
  void findAllReturnsSavedSubset() {
    MessageMetadata a = metadata("q", "a", MessageStatus.ENQUEUED);
    MessageMetadata b = metadata("q", "b", MessageStatus.ENQUEUED);
    svc.save(a, Duration.ofMinutes(1), false);
    svc.save(b, Duration.ofMinutes(1), false);
    var got = svc.findAll(Arrays.asList(a.getId(), b.getId(), "missing"));
    assertEquals(2, got.size());
  }
}
