/*
 * Copyright (c) 2023 Sonu Kumar
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

package com.github.sonus21.rqueue.core;

import static org.apache.commons.lang3.reflect.FieldUtils.writeField;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doReturn;

import com.github.sonus21.TestBase;
import com.github.sonus21.rqueue.CoreUnitTest;
import com.github.sonus21.rqueue.config.RqueueConfig;
import com.github.sonus21.rqueue.core.impl.RqueueMessageEnqueuerImpl;
import com.github.sonus21.rqueue.listener.QueueDetail;
import com.github.sonus21.rqueue.utils.TestUtils;
import com.github.sonus21.rqueue.web.service.RqueueMessageMetadataService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

@CoreUnitTest
class RqueueMessageEnqueuerTest extends TestBase {

  private final String queueName = "test-queue";
  private final QueueDetail queueDetail = TestUtils.createQueueDetail(queueName);
  private final String slowQueue = "slow-queue";
  private final String deadLetterQueueName = "dead-test-queue";
  private final String message = "Test Message";
  @Mock
  private RqueueMessageTemplate rqueueMessageTemplate;
  @Mock
  private RqueueConfig rqueueConfig;
  @Mock
  private RqueueMessageMetadataService rqueueMessageMetadataService;
  private RqueueMessageEnqueuer rqueueMessageEnqueuer;

  @BeforeEach
  public void init() throws IllegalAccessException {
    MockitoAnnotations.openMocks(this);
    rqueueMessageEnqueuer =
        new RqueueMessageEnqueuerImpl(
            rqueueMessageTemplate, new DefaultRqueueMessageConverter(), null);
    EndpointRegistry.delete();
    EndpointRegistry.register(queueDetail);
    writeField(rqueueMessageEnqueuer, "rqueueConfig", rqueueConfig, true);
    writeField(
        rqueueMessageEnqueuer, "rqueueMessageMetadataService", rqueueMessageMetadataService, true);
  }

  @Test
  void enqueueWithNullQueueName() {
    assertThrows(IllegalArgumentException.class, () -> rqueueMessageEnqueuer.enqueue(null, null));
  }

  @Test
  void enqueueWithNullMessage() {
    assertThrows(
        IllegalArgumentException.class, () -> rqueueMessageEnqueuer.enqueue(queueName, null));
  }

  @Test
  void enqueue() {
    doReturn(1L)
        .when(rqueueMessageTemplate)
        .addMessage(eq(queueDetail.getQueueName()), any(RqueueMessage.class));
    assertNotNull(rqueueMessageEnqueuer.enqueue(queueName, message));
  }

  @Test
  void enqueueWithRetry() {
    doReturn(1L)
        .when(rqueueMessageTemplate)
        .addMessage(eq(queueDetail.getQueueName()), any(RqueueMessage.class));
    assertNotNull(rqueueMessageEnqueuer.enqueueWithRetry(queueName, message, 3));
  }

  @Test
  void enqueueWithDelay() {
    doReturn(1L)
        .when(rqueueMessageTemplate)
        .addMessageWithDelay(
            eq(queueDetail.getScheduledQueueName()),
            eq(queueDetail.getScheduledQueueChannelName()),
            any(RqueueMessage.class));
    assertNotNull(rqueueMessageEnqueuer.enqueueIn(queueName, message, 1000L));
  }

  @Test
  void enqueueWithDelayAndRetry() {
    doReturn(1L)
        .when(rqueueMessageTemplate)
        .addMessageWithDelay(
            eq(queueDetail.getScheduledQueueName()),
            eq(queueDetail.getScheduledQueueChannelName()),
            any(RqueueMessage.class));
    assertNotNull(rqueueMessageEnqueuer.enqueueInWithRetry(queueName, message, 3, 1000L));
  }

}
