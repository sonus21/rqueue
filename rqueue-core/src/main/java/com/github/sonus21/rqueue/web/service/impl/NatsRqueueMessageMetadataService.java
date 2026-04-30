/*
 * Copyright (c) 2024-2026 Sonu Kumar
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * You may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 */

package com.github.sonus21.rqueue.web.service.impl;

import com.github.sonus21.rqueue.config.NatsBackendCondition;
import com.github.sonus21.rqueue.core.RqueueMessage;
import com.github.sonus21.rqueue.models.db.MessageMetadata;
import com.github.sonus21.rqueue.models.enums.MessageStatus;
import com.github.sonus21.rqueue.web.service.RqueueMessageMetadataService;
import java.time.Duration;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import org.springframework.context.annotation.Conditional;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;

/**
 * NATS-backend stub {@link RqueueMessageMetadataService} for non-Redis backends. Reads return null/empty;
 * writes are silently ignored. The producer happy path on NATS short-circuits {@code
 * BaseMessageSender.storeMessageMetadata} via the broker capability flag, so metadata writes
 * never reach this stub. Admin/dashboard read methods get an empty view.
 */
@Service
@Conditional(NatsBackendCondition.class)
public class NatsRqueueMessageMetadataService implements RqueueMessageMetadataService {

  @Override
  public MessageMetadata get(String id) {
    return null;
  }

  @Override
  public void delete(String id) {}

  @Override
  public void deleteAll(Collection<String> ids) {}

  @Override
  public List<MessageMetadata> findAll(Collection<String> ids) {
    return Collections.emptyList();
  }

  @Override
  public void save(MessageMetadata messageMetadata, Duration ttl, boolean checkUnique) {}

  @Override
  public MessageMetadata getByMessageId(String queueName, String messageId) {
    return null;
  }

  @Override
  public boolean deleteMessage(String queueName, String messageId, Duration ttl) {
    return false;
  }

  @Override
  public MessageMetadata getOrCreateMessageMetadata(RqueueMessage rqueueMessage) {
    return new MessageMetadata(rqueueMessage, MessageStatus.ENQUEUED);
  }

  @Override
  public Mono<Boolean> saveReactive(MessageMetadata m, Duration ttl, boolean checkUnique) {
    return Mono.just(Boolean.TRUE);
  }

  @Override
  public void deleteQueueMessages(String queueName, long before) {}

  @Override
  public void saveMessageMetadataForQueue(
      String queueName, MessageMetadata messageMetadata, Long ttlInMillisecond) {}

  @Override
  public java.util.List<org.springframework.data.redis.core.ZSetOperations.TypedTuple<MessageMetadata>>
      readMessageMetadataForQueue(String queueName, long start, long end) {
    return Collections.emptyList();
  }
}
