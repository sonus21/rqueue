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
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.nullable;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.github.sonus21.rqueue.core.RqueueMessage;
import com.github.sonus21.rqueue.models.db.MessageMetadata;
import com.github.sonus21.rqueue.models.enums.MessageStatus;
import com.github.sonus21.rqueue.nats.NatsUnitTest;
import com.github.sonus21.rqueue.nats.internal.NatsProvisioner;
import com.github.sonus21.rqueue.serdes.RqueueSerDes;
import io.nats.client.JetStreamApiException;
import io.nats.client.KeyValue;
import io.nats.client.api.KeyValueEntry;
import java.io.IOException;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.data.redis.core.ZSetOperations.TypedTuple;
import reactor.core.publisher.Mono;

/**
 * Unit tests for {@link NatsRqueueMessageMetadataService}: all CRUD paths, deleteMessage
 * tombstone logic, reactive wrapper, queue-scoped reads, and exception swallowing.
 */
@NatsUnitTest
class NatsRqueueMessageMetadataServiceTest {

  private NatsProvisioner provisioner;
  private RqueueSerDes serdes;
  private KeyValue kv;
  private NatsRqueueMessageMetadataService service;

  @BeforeEach
  void setUp() throws IOException, JetStreamApiException, InterruptedException {
    provisioner = mock(NatsProvisioner.class);
    serdes = mock(RqueueSerDes.class);
    kv = mock(KeyValue.class);
    when(provisioner.ensureKv(anyString(), nullable(Duration.class))).thenReturn(kv);
    service = new NatsRqueueMessageMetadataService(provisioner, serdes);
  }

  private static KeyValueEntry entry(byte[] value) {
    return mock(
        KeyValueEntry.class, inv -> "getValue".equals(inv.getMethod().getName()) ? value : null);
  }

  private MessageMetadata meta(String id) {
    return new MessageMetadata(id, MessageStatus.ENQUEUED);
  }

  // ---- get ---------------------------------------------------------

  @Test
  void get_returnsDeserialisedMetadata()
      throws IOException, JetStreamApiException, InterruptedException {
    byte[] bytes = "{}".getBytes();
    MessageMetadata m = meta("meta-1");
    when(kv.get("meta-1")).thenReturn(entry(bytes));
    when(serdes.deserialize(bytes, MessageMetadata.class)).thenReturn(m);

    MessageMetadata result = service.get("meta-1");

    assertNotNull(result);
    assertEquals("meta-1", result.getId());
  }

  @Test
  void get_entryNotFound_returnsNull()
      throws IOException, JetStreamApiException, InterruptedException {
    when(kv.get(anyString())).thenReturn(null);
    assertNull(service.get("ghost"));
  }

  @Test
  void get_entryNullValue_returnsNull()
      throws IOException, JetStreamApiException, InterruptedException {
    KeyValueEntry e = mock(KeyValueEntry.class);
    when(e.getValue()).thenReturn(null);
    when(kv.get(anyString())).thenReturn(e);
    assertNull(service.get("meta-1"));
  }

  @Test
  void get_ioException_returnsNull()
      throws IOException, JetStreamApiException, InterruptedException {
    when(kv.get(anyString())).thenThrow(new IOException("network error"));
    assertNull(service.get("meta-1"));
  }

  // ---- delete ------------------------------------------------------

  @Test
  void delete_callsKvDelete() throws IOException, JetStreamApiException, InterruptedException {
    service.delete("meta-1");
    verify(kv).delete("meta-1");
  }

  @Test
  void delete_ioException_isSwallowed()
      throws IOException, JetStreamApiException, InterruptedException {
    doThrow(new IOException("kv down")).when(kv).delete(anyString());
    service.delete("meta-1"); // must not throw
  }

  @Test
  void delete_sanitizesKey() throws IOException, JetStreamApiException, InterruptedException {
    service.delete("a$b#c");
    verify(kv).delete("a_b_c");
    verify(kv, never()).delete("a$b#c");
  }

  // ---- deleteAll ---------------------------------------------------

  @Test
  void deleteAll_callsDeleteForEachId()
      throws IOException, JetStreamApiException, InterruptedException {
    service.deleteAll(Arrays.asList("meta-1", "meta-2"));
    verify(kv).delete("meta-1");
    verify(kv).delete("meta-2");
  }

  @Test
  void deleteAll_emptyCollection_isNoOp()
      throws IOException, JetStreamApiException, InterruptedException {
    service.deleteAll(Collections.emptyList());
    verify(kv, never()).delete(anyString());
  }

  // ---- findAll -----------------------------------------------------

  @Test
  void findAll_returnsPresent() throws IOException, JetStreamApiException, InterruptedException {
    byte[] bytes = "{}".getBytes();
    MessageMetadata m = meta("m1");
    when(kv.get("m1")).thenReturn(entry(bytes));
    when(kv.get("m2")).thenReturn(null);
    when(serdes.deserialize(bytes, MessageMetadata.class)).thenReturn(m);

    List<MessageMetadata> result = service.findAll(Arrays.asList("m1", "m2"));

    assertEquals(1, result.size());
    assertEquals("m1", result.get(0).getId());
  }

  // ---- save --------------------------------------------------------

  @Test
  void save_putsToKv() throws IOException, JetStreamApiException, InterruptedException {
    MessageMetadata m = meta("meta-1");
    when(serdes.serialize(m)).thenReturn("{}".getBytes());

    service.save(m, Duration.ofMinutes(10), false);

    verify(kv, times(1)).put(eq("meta-1"), any(byte[].class));
  }

  @Test
  void save_ioException_isSwallowed()
      throws IOException, JetStreamApiException, InterruptedException {
    MessageMetadata m = meta("meta-1");
    when(serdes.serialize(m)).thenReturn("{}".getBytes());
    doThrow(new IOException("kv down")).when(kv).put(anyString(), any(byte[].class));

    service.save(m, null, false); // must not throw
  }

  // ---- getByMessageId ----------------------------------------------

  @Test
  void getByMessageId_delegatesToGetWithComposedKey()
      throws IOException, JetStreamApiException, InterruptedException {
    when(kv.get(anyString())).thenReturn(null);

    service.getByMessageId("orders", "msg-1");

    // Should call kv.get with a composed meta-id
    verify(kv).get(anyString());
  }

  // ---- deleteMessage ----------------------------------------------

  @Test
  void deleteMessage_existingMetadata_marksDeletedAndSaves()
      throws IOException, JetStreamApiException, InterruptedException {
    MessageMetadata existing = meta("existing-meta");
    byte[] bytes = "{}".getBytes();
    when(kv.get(anyString())).thenReturn(entry(bytes));
    when(serdes.deserialize(bytes, MessageMetadata.class)).thenReturn(existing);
    when(serdes.serialize(any(MessageMetadata.class))).thenReturn("{}".getBytes());

    boolean result = service.deleteMessage("orders", "msg-1", Duration.ofMinutes(5));

    assertTrue(result);
    assertTrue(existing.isDeleted());
    assertEquals(MessageStatus.DELETED, existing.getStatus());
    verify(kv, times(1)).put(anyString(), any(byte[].class));
  }

  @Test
  void deleteMessage_missingMetadata_createsTombstoneAndSaves()
      throws IOException, JetStreamApiException, InterruptedException {
    when(kv.get(anyString())).thenReturn(null); // no existing metadata
    when(serdes.serialize(any(MessageMetadata.class))).thenReturn("{}".getBytes());

    boolean result = service.deleteMessage("orders", "msg-new", Duration.ofMinutes(5));

    assertTrue(result);
    // tombstone must be saved
    verify(kv, times(1)).put(anyString(), any(byte[].class));
  }

  @Test
  void deleteMessage_alwaysReturnsTrue()
      throws IOException, JetStreamApiException, InterruptedException {
    when(kv.get(anyString())).thenReturn(null);
    when(serdes.serialize(any(MessageMetadata.class))).thenReturn("{}".getBytes());

    assertTrue(service.deleteMessage("q", "m", null));
  }

  // ---- getOrCreateMessageMetadata ---------------------------------

  @Test
  void getOrCreateMessageMetadata_existing_returnsIt()
      throws IOException, JetStreamApiException, InterruptedException {
    MessageMetadata existing = meta("existing");
    byte[] bytes = "{}".getBytes();
    when(kv.get(anyString())).thenReturn(entry(bytes));
    when(serdes.deserialize(bytes, MessageMetadata.class)).thenReturn(existing);

    RqueueMessage msg = RqueueMessage.builder()
        .id("msg-1")
        .queueName("orders")
        .message("payload")
        .queuedTime(System.currentTimeMillis())
        .build();
    MessageMetadata result = service.getOrCreateMessageMetadata(msg);

    assertNotNull(result);
    assertEquals("existing", result.getId());
  }

  @Test
  void getOrCreateMessageMetadata_notFound_createsNewWithEnqueuedStatus()
      throws IOException, JetStreamApiException, InterruptedException {
    when(kv.get(anyString())).thenReturn(null);

    RqueueMessage msg = RqueueMessage.builder()
        .id("msg-1")
        .queueName("orders")
        .message("payload")
        .queuedTime(System.currentTimeMillis())
        .build();
    MessageMetadata result = service.getOrCreateMessageMetadata(msg);

    assertNotNull(result);
    assertEquals(MessageStatus.ENQUEUED, result.getStatus());
  }

  // ---- saveReactive -----------------------------------------------

  @Test
  void saveReactive_wrapsAndReturnsTrue()
      throws IOException, JetStreamApiException, InterruptedException {
    MessageMetadata m = meta("meta-1");
    when(serdes.serialize(m)).thenReturn("{}".getBytes());

    Mono<Boolean> mono = service.saveReactive(m, Duration.ofMinutes(5), false);
    Boolean result = mono.block();

    assertTrue(result);
    verify(kv, times(1)).put(anyString(), any(byte[].class));
  }

  // ---- readMessageMetadataForQueue --------------------------------

  @Test
  void readMessageMetadataForQueue_returnsMatchingKeys()
      throws IOException, JetStreamApiException, InterruptedException {
    byte[] bytes = "{}".getBytes();
    MessageMetadata m = meta("orders_msg1");
    m.setUpdatedOn(1000L);
    when(kv.keys()).thenReturn(Arrays.asList("orders_msg1", "other_msg2"));
    when(kv.get("orders_msg1")).thenReturn(entry(bytes));
    when(serdes.deserialize(bytes, MessageMetadata.class)).thenReturn(m);

    List<TypedTuple<MessageMetadata>> result =
        service.readMessageMetadataForQueue("orders", 0, 100);

    assertEquals(1, result.size());
    assertEquals(1000.0, result.get(0).getScore());
  }

  @Test
  void readMessageMetadataForQueue_ioException_returnsEmpty()
      throws IOException, JetStreamApiException, InterruptedException {
    when(kv.keys()).thenThrow(new IOException("kv down"));

    List<TypedTuple<MessageMetadata>> result =
        service.readMessageMetadataForQueue("orders", 0, 100);

    assertTrue(result.isEmpty());
  }

  @Test
  void readMessageMetadataForQueue_interruptedException_setsInterruptFlag()
      throws IOException, JetStreamApiException, InterruptedException {
    when(kv.keys()).thenThrow(new InterruptedException("interrupted"));

    service.readMessageMetadataForQueue("orders", 0, 100);

    assertTrue(Thread.currentThread().isInterrupted());
    Thread.interrupted(); // clear
  }

  // ---- deleteQueueMessages ----------------------------------------

  @Test
  void deleteQueueMessages_deletesEntriesOlderThanBefore()
      throws IOException, JetStreamApiException, InterruptedException {
    byte[] bytes = "{}".getBytes();
    MessageMetadata old = meta("orders_old");
    old.setUpdatedOn(500L);
    MessageMetadata fresh = meta("orders_fresh");
    fresh.setUpdatedOn(2000L);

    when(kv.keys()).thenReturn(Arrays.asList("orders_old", "orders_fresh"));
    when(kv.get("orders_old")).thenReturn(entry(bytes));
    when(kv.get("orders_fresh")).thenReturn(entry(bytes));
    when(serdes.deserialize(bytes, MessageMetadata.class)).thenReturn(old).thenReturn(fresh);

    service.deleteQueueMessages("orders", 1000L);

    verify(kv).delete("orders_old");
    verify(kv, never()).delete("orders_fresh");
  }

  @Test
  void deleteQueueMessages_interruptedException_setsInterruptFlag()
      throws IOException, JetStreamApiException, InterruptedException {
    when(kv.keys()).thenThrow(new InterruptedException("interrupted"));

    service.deleteQueueMessages("orders", 1000L);

    assertTrue(Thread.currentThread().isInterrupted());
    Thread.interrupted(); // clear
  }

  @Test
  void deleteQueueMessages_ioException_isSwallowed()
      throws IOException, JetStreamApiException, InterruptedException {
    when(kv.keys()).thenThrow(new IOException("kv down"));
    service.deleteQueueMessages("orders", 1000L); // must not throw
  }

  // ---- saveMessageMetadataForQueue --------------------------------

  @Test
  void saveMessageMetadataForQueue_delegatesToSave()
      throws IOException, JetStreamApiException, InterruptedException {
    MessageMetadata m = meta("meta-1");
    when(serdes.serialize(m)).thenReturn("{}".getBytes());

    service.saveMessageMetadataForQueue("orders", m, 60_000L);

    verify(kv, times(1)).put(anyString(), any(byte[].class));
  }

  @Test
  void saveMessageMetadataForQueue_nullTtl_doesNotThrow()
      throws IOException, JetStreamApiException, InterruptedException {
    MessageMetadata m = meta("meta-1");
    when(serdes.serialize(m)).thenReturn("{}".getBytes());

    service.saveMessageMetadataForQueue("orders", m, null);

    verify(kv, times(1)).put(anyString(), any(byte[].class));
  }
}
