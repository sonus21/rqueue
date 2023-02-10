/*
 * Copyright (c) 2019-2023 Sonu Kumar
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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.sonus21.TestBase;
import com.github.sonus21.rqueue.CoreUnitTest;
import com.github.sonus21.rqueue.utils.SerializationUtils;
import java.util.UUID;
import org.junit.jupiter.api.Test;

@CoreUnitTest
class RqueueMessageTest extends TestBase {

  private final ObjectMapper objectMapper = SerializationUtils.createObjectMapper();
  private final String queueName = "test-queue";
  private final String queueMessage = "This is a test message";
  private final Integer retryCount = 3;
  private final long delay = 100L;

  @Test
  void setReEnqueuedAt() {
    RqueueMessage message =
        RqueueMessage.builder()
            .queueName(queueName)
            .message(queueMessage)
            .retryCount(retryCount)
            .processAt(System.currentTimeMillis() + delay)
            .queuedTime(System.nanoTime())
            .build();
    Long time = System.currentTimeMillis() - delay;
    message.setReEnqueuedAt(time);
    assertEquals(message.getReEnqueuedAt(), time);
  }

  @Test
  void objectEquality() throws JsonProcessingException {
    RqueueMessage message =
        RqueueMessage.builder()
            .id(UUID.randomUUID().toString())
            .queueName(queueName)
            .message(queueMessage)
            .retryCount(retryCount)
            .processAt(System.currentTimeMillis() + delay)
            .queuedTime(System.nanoTime())
            .build();
    String stringMessage = objectMapper.writeValueAsString(message);
    assertEquals(message, objectMapper.readValue(stringMessage, RqueueMessage.class));
  }

  @Test
  void objectEqualityWithoutDelay() throws JsonProcessingException {
    RqueueMessage message =
        RqueueMessage.builder()
            .id(UUID.randomUUID().toString())
            .queueName(queueName)
            .message(queueMessage)
            .retryCount(retryCount)
            .processAt(System.currentTimeMillis())
            .queuedTime(System.nanoTime())
            .build();
    String stringMessage = objectMapper.writeValueAsString(message);
    assertEquals(message, objectMapper.readValue(stringMessage, RqueueMessage.class));
  }

  @Test
  void objectEqualityWithDifferentObject() {
    RqueueMessage message =
        RqueueMessage.builder()
            .queueName(queueName)
            .message(queueMessage)
            .retryCount(retryCount)
            .processAt(System.currentTimeMillis())
            .queuedTime(System.nanoTime())
            .build();
    assertNotEquals(message, new Object());
  }

  @Test
  void objectEqualityWithDifferentId() {
    RqueueMessage message =
        RqueueMessage.builder()
            .id(UUID.randomUUID().toString())
            .queueName(queueName)
            .message(queueMessage)
            .retryCount(retryCount)
            .processAt(System.currentTimeMillis())
            .queuedTime(System.nanoTime())
            .build();

    RqueueMessage message2 =
        RqueueMessage.builder()
            .id("x" + UUID.randomUUID().toString())
            .queueName(queueName)
            .message(queueMessage)
            .retryCount(retryCount)
            .processAt(System.currentTimeMillis())
            .queuedTime(System.nanoTime())
            .build();
    assertNotEquals(message, message2);
  }

  @Test
  void RqueueMessageToString() {
    RqueueMessage message =
        RqueueMessage.builder()
            .id(UUID.randomUUID().toString())
            .queueName(queueName)
            .message(queueMessage)
            .retryCount(retryCount)
            .processAt(System.currentTimeMillis())
            .queuedTime(System.nanoTime())
            .build();
    String toString =
        "RqueueMessage(id="
            + message.getId()
            + ", queueName=test-queue, message=This is a test message, retryCount="
            + retryCount
            + ", queuedTime="
            + message.getQueuedTime()
            + ", processAt="
            + message.getProcessAt()
            + ", reEnqueuedAt=null, failureCount=0, sourceQueueFailureCount=0, sourceQueueName=null, period=0)";
    assertEquals(toString, message.toString());
  }

  @Test
  void isPeriodic() {
    RqueueMessage message =
        RqueueMessage.builder()
            .id(UUID.randomUUID().toString())
            .queueName(queueName)
            .message(queueMessage)
            .retryCount(retryCount)
            .processAt(System.currentTimeMillis())
            .queuedTime(System.nanoTime())
            .period(10000)
            .build();
    assertTrue(message.isPeriodic());
    message.setPeriod(0);
    assertFalse(message.isPeriodic());
  }
}
