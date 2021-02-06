/*
 *  Copyright 2021 Sonu Kumar
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *         https://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and limitations under the License.
 *
 */

package com.github.sonus21.rqueue.core.impl;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

import com.github.sonus21.rqueue.common.RqueueLockManager;
import com.github.sonus21.rqueue.config.RqueueConfig;
import com.github.sonus21.rqueue.core.DefaultRqueueMessageConverter;
import com.github.sonus21.rqueue.core.EndpointRegistry;
import com.github.sonus21.rqueue.core.RqueueEndpointManager;
import com.github.sonus21.rqueue.core.RqueueMessageTemplate;
import com.github.sonus21.rqueue.listener.RqueueMessageHeaders;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.messaging.MessageHeaders;
import org.springframework.messaging.converter.MessageConverter;

class RqueueEndpointManagerImplTest {

  private final RqueueMessageTemplate messageTemplate = mock(RqueueMessageTemplate.class);
  private final RqueueLockManager rqueueLockManager = mock(RqueueLockManager.class);
  private final String queue = "test-queue";
  private final RqueueConfig rqueueConfig =
      new RqueueConfig(mock(RedisConnectionFactory.class), false, 1);
  MessageConverter messageConverter = new DefaultRqueueMessageConverter();
  MessageHeaders messageHeaders = RqueueMessageHeaders.emptyMessageHeaders();
  private final RqueueEndpointManager rqueueEndpointManager =
      new RqueueEndpointManagerImpl(messageTemplate, messageConverter, messageHeaders);

  @BeforeEach
  public void init() throws IllegalAccessException {
    FieldUtils.writeField(rqueueEndpointManager, "rqueueConfig", rqueueConfig, true);
    EndpointRegistry.delete();
  }

  @AfterEach
  public void clean() {
    EndpointRegistry.delete();
  }

  @Test
  void isQueueRegistered() {
    assertFalse(rqueueEndpointManager.isQueueRegistered(queue));
    rqueueEndpointManager.registerQueue(queue);
    assertTrue(rqueueEndpointManager.isQueueRegistered(queue));
  }

  @Test
  void getQueueConfig() {
    rqueueEndpointManager.registerQueue(queue, "high", "medium", "low");
    assertEquals(4, rqueueEndpointManager.getQueueConfig(queue).size());
  }
}
