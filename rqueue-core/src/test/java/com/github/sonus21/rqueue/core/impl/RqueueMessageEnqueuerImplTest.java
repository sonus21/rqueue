/*
 * Copyright (c) 2021-2023 Sonu Kumar
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

import static org.junit.jupiter.api.Assertions.assertThrows;

import com.github.sonus21.TestBase;
import com.github.sonus21.rqueue.CoreUnitTest;
import com.github.sonus21.rqueue.config.RqueueConfig;
import com.github.sonus21.rqueue.core.DefaultRqueueMessageConverter;
import com.github.sonus21.rqueue.core.EndpointRegistry;
import com.github.sonus21.rqueue.core.RqueueMessageEnqueuer;
import com.github.sonus21.rqueue.core.RqueueMessageTemplate;
import com.github.sonus21.rqueue.listener.QueueDetail;
import com.github.sonus21.rqueue.listener.RqueueMessageHeaders;
import com.github.sonus21.rqueue.utils.Constants;
import com.github.sonus21.rqueue.utils.TestUtils;
import com.github.sonus21.rqueue.web.service.RqueueMessageMetadataService;
import java.util.UUID;
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
class RqueueMessageEnqueuerImplTest extends TestBase {

  private static final String queue = "test-queue";
  private static final QueueDetail queueDetail = TestUtils.createQueueDetail(queue);
  MessageConverter messageConverter = new DefaultRqueueMessageConverter();
  MessageHeaders messageHeaders = RqueueMessageHeaders.emptyMessageHeaders();
  @Mock
  private RqueueMessageMetadataService rqueueMessageMetadataService;
  @Mock
  private RqueueMessageTemplate messageTemplate;
  @Mock
  private RqueueConfig rqueueConfig;
  private RqueueMessageEnqueuer rqueueMessageEnqueuer;

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
    rqueueMessageEnqueuer =
        new RqueueMessageEnqueuerImpl(messageTemplate, messageConverter, messageHeaders);
    FieldUtils.writeField(rqueueMessageEnqueuer, "rqueueConfig", rqueueConfig, true);
    FieldUtils.writeField(rqueueMessageEnqueuer, "rqueueMessageMetadataService",
        rqueueMessageMetadataService, true);
  }

  @Test
  void enqueueWithRetry() {
    assertThrows(
        IllegalArgumentException.class,
        () -> rqueueMessageEnqueuer.enqueueWithRetry(queue, "test", -1));

    assertThrows(
        IllegalArgumentException.class,
        () -> rqueueMessageEnqueuer.enqueueWithRetry(queue, null, 1));

    assertThrows(
        IllegalArgumentException.class,
        () -> rqueueMessageEnqueuer.enqueueWithRetry(null, "test", 10));

    rqueueMessageEnqueuer.enqueueWithRetry(queue, "test-message", 1);
  }

  @Test
  void enqueueInWithRetry() {
    assertThrows(
        IllegalArgumentException.class,
        () -> rqueueMessageEnqueuer.enqueueInWithRetry(queue, "test", 1, -1L));

    assertThrows(
        IllegalArgumentException.class,
        () -> rqueueMessageEnqueuer.enqueueInWithRetry(queue, "test", -1, 1000L));

    assertThrows(
        IllegalArgumentException.class,
        () -> rqueueMessageEnqueuer.enqueueInWithRetry(queue, null, 1, 1000L));

    assertThrows(
        IllegalArgumentException.class,
        () -> rqueueMessageEnqueuer.enqueueInWithRetry(null, "test", 1, 1000L));

    rqueueMessageEnqueuer.enqueueInWithRetry(queue, "test-message", 1, 1000L);
  }

  @Test
  void enqueuePeriodic() {
    String id = UUID.randomUUID().toString();
    assertThrows(
        IllegalArgumentException.class,
        () -> rqueueMessageEnqueuer.enqueuePeriodic(queue, id, "test", -1L));

    assertThrows(
        IllegalArgumentException.class,
        () -> rqueueMessageEnqueuer.enqueuePeriodic(queue, id, null, 5 * Constants.ONE_MILLI));

    assertThrows(
        IllegalArgumentException.class,
        () -> rqueueMessageEnqueuer.enqueuePeriodic(queue, null, "test", 5 * Constants.ONE_MILLI));

    assertThrows(
        IllegalArgumentException.class,
        () -> rqueueMessageEnqueuer.enqueuePeriodic(null, id, "test", 5 * Constants.ONE_MILLI));

    rqueueMessageEnqueuer.enqueuePeriodic(queue, id, "test-message", 5 * Constants.ONE_MILLI);
  }
}
