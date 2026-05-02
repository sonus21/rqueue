/*
 * Copyright (c) 2020-2026 Sonu Kumar
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
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.github.sonus21.TestBase;
import com.github.sonus21.rqueue.CoreUnitTest;
import com.github.sonus21.rqueue.config.RqueueConfig;
import com.github.sonus21.rqueue.core.DefaultRqueueMessageConverter;
import com.github.sonus21.rqueue.core.EndpointRegistry;
import com.github.sonus21.rqueue.core.RqueueMessageEnqueuer;
import com.github.sonus21.rqueue.core.RqueueMessageIdGenerator;
import com.github.sonus21.rqueue.core.RqueueMessageTemplate;
import com.github.sonus21.rqueue.core.spi.Capabilities;
import com.github.sonus21.rqueue.core.spi.MessageBroker;
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

@CoreUnitTest
class BaseMessageSenderMetadataTest extends TestBase {

  private static final String queue = "test-queue-metadata";
  private static final QueueDetail queueDetail = TestUtils.createQueueDetail(queue);
  private static final RqueueMessageIdGenerator FIXED_MESSAGE_ID_GENERATOR = () -> "metadata-id";

  private final MessageConverter messageConverter = new DefaultRqueueMessageConverter();
  private final MessageHeaders messageHeaders = RqueueMessageHeaders.emptyMessageHeaders();

  @Mock
  private RqueueMessageMetadataService rqueueMessageMetadataService;

  @Mock
  private RqueueMessageTemplate messageTemplate;

  @Mock
  private MessageBroker messageBroker;

  private RqueueConfig rqueueConfig;
  private RqueueMessageEnqueuer enqueuer;

  @BeforeAll
  public static void init0() {
    EndpointRegistry.delete();
    EndpointRegistry.register(queueDetail);
  }

  @AfterAll
  public static void clean() {
    EndpointRegistry.delete();
  }

  @BeforeEach
  public void init() throws IllegalAccessException {
    MockitoAnnotations.openMocks(this);
    rqueueConfig = new RqueueConfig(null, null, true, 2);
    rqueueConfig.setMessageDurabilityInMinute(10080);
    enqueuer = new RqueueMessageEnqueuerImpl(
        messageTemplate,
        messageBroker,
        messageConverter,
        messageHeaders,
        FIXED_MESSAGE_ID_GENERATOR);
    FieldUtils.writeField(enqueuer, "rqueueConfig", rqueueConfig, true);
    FieldUtils.writeField(
        enqueuer, "rqueueMessageMetadataService", rqueueMessageMetadataService, true);
    lenient().doNothing().when(rqueueMessageMetadataService).save(any(), any(), anyBoolean());
  }

  @Test
  void redisCapabilitiesSaveMetadata() {
    when(messageBroker.capabilities()).thenReturn(Capabilities.REDIS_DEFAULTS);
    String id = enqueuer.enqueue(queue, "redis-payload");
    assertEquals("metadata-id", id);
    verify(rqueueMessageMetadataService).save(any(), any(), anyBoolean());
  }

  @Test
  void natsLikeCapabilitiesSkipMetadata() {
    Capabilities caps = new Capabilities(true, false, false, false);
    when(messageBroker.capabilities()).thenReturn(caps);

    String id = enqueuer.enqueue(queue, "nats-payload");
    assertEquals("metadata-id", id);
    verify(rqueueMessageMetadataService, never()).save(any(), any(), anyBoolean());
  }
}
