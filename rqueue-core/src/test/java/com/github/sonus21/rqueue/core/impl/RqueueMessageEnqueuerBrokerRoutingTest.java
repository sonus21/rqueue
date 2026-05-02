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

package com.github.sonus21.rqueue.core.impl;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.github.sonus21.TestBase;
import com.github.sonus21.rqueue.CoreUnitTest;
import com.github.sonus21.rqueue.config.RqueueConfig;
import com.github.sonus21.rqueue.core.DefaultRqueueMessageConverter;
import com.github.sonus21.rqueue.core.EndpointRegistry;
import com.github.sonus21.rqueue.core.RqueueMessage;
import com.github.sonus21.rqueue.core.RqueueMessageEnqueuer;
import com.github.sonus21.rqueue.core.RqueueMessageIdGenerator;
import com.github.sonus21.rqueue.core.RqueueMessageTemplate;
import com.github.sonus21.rqueue.core.spi.Capabilities;
import com.github.sonus21.rqueue.core.spi.MessageBroker;
import com.github.sonus21.rqueue.core.spi.redis.RedisMessageBroker;
import com.github.sonus21.rqueue.listener.QueueDetail;
import com.github.sonus21.rqueue.listener.RqueueMessageHeaders;
import com.github.sonus21.rqueue.service.RqueueMessageMetadataService;
import com.github.sonus21.rqueue.utils.TestUtils;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.springframework.messaging.MessageHeaders;
import org.springframework.messaging.converter.MessageConverter;

/**
 * Pins the non-reactive enqueue path. Verifies that {@code BaseMessageSender.enqueue()} always
 * routes through the {@link MessageBroker} regardless of backend, and never falls through to a
 * Redis-shaped {@code messageTemplate.addMessage()} call. Regression coverage for the NPE that
 * surfaced when a NATS-backed enqueuer was constructed against a template with a {@code null}
 * {@code RedisTemplate}.
 */
@CoreUnitTest
class RqueueMessageEnqueuerBrokerRoutingTest extends TestBase {

  private static final String queue = "broker-routing-queue-sync";
  private static final QueueDetail queueDetail = TestUtils.createQueueDetail(queue);
  private static final RqueueMessageIdGenerator FIXED_ID = () -> "fixed-id";
  // NATS-shaped: not the primary-handler-dispatch (Redis) capability set.
  private static final Capabilities NATS_LIKE = new Capabilities(true, false, false, false);

  private final MessageConverter messageConverter = new DefaultRqueueMessageConverter();
  private final MessageHeaders messageHeaders = RqueueMessageHeaders.emptyMessageHeaders();

  @Mock
  private RqueueMessageMetadataService rqueueMessageMetadataService;

  @Mock
  private RqueueMessageTemplate messageTemplate;

  @Mock
  private MessageBroker messageBroker;

  @BeforeAll
  static void init0() {
    EndpointRegistry.delete();
    EndpointRegistry.register(queueDetail);
  }

  @AfterAll
  static void clean() {
    EndpointRegistry.delete();
  }

  @BeforeEach
  void init() {
    MockitoAnnotations.openMocks(this);
    lenient().doNothing().when(rqueueMessageMetadataService).save(any(), any(), anyBoolean());
  }

  private RqueueMessageEnqueuer newEnqueuer(MessageBroker broker) throws IllegalAccessException {
    RqueueConfig rqueueConfig = new RqueueConfig(null, null, true, 2);
    rqueueConfig.setMessageDurabilityInMinute(10080);
    RqueueMessageEnqueuer enqueuer = new RqueueMessageEnqueuerImpl(
        messageTemplate, broker, messageConverter, messageHeaders, FIXED_ID);
    FieldUtils.writeField(enqueuer, "rqueueConfig", rqueueConfig, true);
    FieldUtils.writeField(
        enqueuer, "rqueueMessageMetadataService", rqueueMessageMetadataService, true);
    return enqueuer;
  }

  @Test
  void enqueue_routesThroughBroker_natsBackend() throws IllegalAccessException {
    when(messageBroker.capabilities()).thenReturn(NATS_LIKE);
    RqueueMessageEnqueuer enqueuer = newEnqueuer(messageBroker);

    String id = enqueuer.enqueue(queue, "payload");

    assertEquals("fixed-id", id);
    verify(messageBroker, times(1))
        .enqueue(any(QueueDetail.class), isNull(), any(RqueueMessage.class));
    // The Redis-shaped template path must never be touched — that was the original NPE source.
    verify(messageTemplate, never()).addMessage(any(), any());
    verify(messageTemplate, never()).addMessageWithDelay(any(), any(), any());
    // NATS capabilities advertise !usesPrimaryHandlerDispatch — metadata save is skipped.
    verify(rqueueMessageMetadataService, never()).save(any(), any(), anyBoolean());
  }

  @Test
  void enqueueIn_routesThroughBrokerDelayed_natsBackend() throws IllegalAccessException {
    when(messageBroker.capabilities()).thenReturn(NATS_LIKE);
    RqueueMessageEnqueuer enqueuer = newEnqueuer(messageBroker);

    String id = enqueuer.enqueueIn(queue, "payload", 5_000L);

    assertEquals("fixed-id", id);
    verify(messageBroker, times(1))
        .enqueueWithDelay(any(QueueDetail.class), any(RqueueMessage.class), eq(5_000L));
    verify(messageTemplate, never()).addMessageWithDelay(any(), any(), any());
  }

  @Test
  void enqueue_routesThroughRedisBroker_redisBackend() throws IllegalAccessException {
    // Redis path uses a real RedisMessageBroker that delegates to template.addMessage. No
    // direct messageTemplate calls from BaseMessageSender — they all go through the broker.
    RedisMessageBroker redisBroker = new RedisMessageBroker(messageTemplate);
    RqueueMessageEnqueuer enqueuer = newEnqueuer(redisBroker);

    String id = enqueuer.enqueue(queue, "payload");

    assertEquals("fixed-id", id);
    verify(messageTemplate, times(1))
        .addMessage(eq(queueDetail.getQueueName()), any(RqueueMessage.class));
    verify(messageTemplate, never()).addMessageWithDelay(any(), any(), any());
    // Redis capabilities have usesPrimaryHandlerDispatch=true — metadata is saved.
    verify(rqueueMessageMetadataService).save(any(), any(), anyBoolean());
  }
}
