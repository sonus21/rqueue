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
 *
 */

package com.github.sonus21.rqueue.core.impl;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.eq;
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
import com.github.sonus21.rqueue.core.RqueueMessageIdGenerator;
import com.github.sonus21.rqueue.core.RqueueMessageTemplate;
import com.github.sonus21.rqueue.core.spi.MessageBroker;
import com.github.sonus21.rqueue.listener.QueueDetail;
import com.github.sonus21.rqueue.listener.RqueueMessageHeaders;
import com.github.sonus21.rqueue.utils.TestUtils;
import com.github.sonus21.rqueue.web.service.RqueueMessageMetadataService;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.springframework.messaging.MessageHeaders;
import org.springframework.messaging.converter.MessageConverter;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

@CoreUnitTest
class ReactiveRqueueMessageEnqueuerBrokerRoutingTest extends TestBase {

  private static final String queue = "broker-routing-queue";
  private static final QueueDetail queueDetail = TestUtils.createQueueDetail(queue);
  private static final RqueueMessageIdGenerator FIXED_ID = () -> "fixed-id";

  private final MessageConverter messageConverter = new DefaultRqueueMessageConverter();
  private final MessageHeaders messageHeaders = RqueueMessageHeaders.emptyMessageHeaders();

  @Mock
  private RqueueMessageMetadataService rqueueMessageMetadataService;

  @Mock
  private RqueueMessageTemplate messageTemplate;

  @Mock
  private MessageBroker messageBroker;

  private ReactiveRqueueMessageEnqueuerImpl enqueuer;

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
  void init() throws IllegalAccessException {
    MockitoAnnotations.openMocks(this);
    RqueueConfig rqueueConfig = new RqueueConfig(null, null, true, 2);
    rqueueConfig.setMessageDurabilityInMinute(10080);
    enqueuer = new ReactiveRqueueMessageEnqueuerImpl(
        messageTemplate, messageConverter, messageHeaders, FIXED_ID);
    FieldUtils.writeField(enqueuer, "rqueueConfig", rqueueConfig, true);
    FieldUtils.writeField(
        enqueuer, "rqueueMessageMetadataService", rqueueMessageMetadataService, true);
    lenient()
        .when(rqueueMessageMetadataService.saveReactive(any(), any(), anyBoolean()))
        .thenReturn(Mono.just(Boolean.TRUE));
  }

  @Test
  void enqueueReactive_routesThroughBroker_whenBrokerSet() {
    enqueuer.setMessageBroker(messageBroker);
    when(messageBroker.enqueueReactive(any(QueueDetail.class), any(RqueueMessage.class)))
        .thenReturn(Mono.empty());

    StepVerifier.create(enqueuer.enqueue(queue, "payload"))
        .expectNext("fixed-id")
        .verifyComplete();

    verify(messageBroker, times(1))
        .enqueueReactive(any(QueueDetail.class), any(RqueueMessage.class));
    verify(messageTemplate, never()).addReactiveMessage(eq(queue), any());
  }

  @Test
  void enqueueInReactive_routesThroughBrokerDelayed_whenBrokerSet() {
    enqueuer.setMessageBroker(messageBroker);
    when(messageBroker.enqueueWithDelayReactive(
            any(QueueDetail.class), any(RqueueMessage.class), eq(5_000L)))
        .thenReturn(Mono.empty());

    StepVerifier.create(enqueuer.enqueueIn(queue, "payload", 5_000L))
        .expectNext("fixed-id")
        .verifyComplete();

    verify(messageBroker, times(1))
        .enqueueWithDelayReactive(any(QueueDetail.class), any(RqueueMessage.class), eq(5_000L));
    verify(messageTemplate, never()).addReactiveMessageWithDelay(any(), any(), any());
  }

  @Test
  void enqueueReactive_fallsBackToRedisTemplate_whenBrokerNull() {
    when(messageTemplate.addReactiveMessage(
            eq(queueDetail.getQueueName()), any(RqueueMessage.class)))
        .thenReturn(Mono.just(1L));

    StepVerifier.create(enqueuer.enqueue(queue, "payload"))
        .expectNext("fixed-id")
        .verifyComplete();

    verify(messageTemplate, times(1))
        .addReactiveMessage(eq(queueDetail.getQueueName()), any(RqueueMessage.class));
    verify(messageBroker, never()).enqueueReactive(any(), any());
  }
}
