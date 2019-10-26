/*
 * Copyright (c)  2019-2019, Sonu Kumar
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *       https://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */

package com.github.sonus21.rqueue.core;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.Test;

public class RqueueMessageTest {
  private ObjectMapper objectMapper = new ObjectMapper();
  private String queueName = "test-queue";
  private String queueMessage = "This is a test message";
  private Integer retryCount = 3;
  private long delay = 100L;

  @Test
  public void checkIdAndProcessAtAreNotSet() {
    RqueueMessage message = new RqueueMessage(queueName, queueMessage, retryCount, null);
    assertNull(message.getId());
    assertEquals(0, message.getProcessAt());
  }

  @Test
  public void checkIdAndProcessAtAreSet() {
    RqueueMessage message = new RqueueMessage(queueName, queueMessage, retryCount, delay);
    assertTrue(message.getId().startsWith(queueName));
    assertTrue(
        message.getProcessAt() <= System.currentTimeMillis() + 100
            && message.getProcessAt() > System.currentTimeMillis());
  }

  @Test
  public void testSetReEnqueuedAt() throws JsonProcessingException {
    RqueueMessage message = new RqueueMessage(queueName, queueMessage, retryCount, delay);
    Long time = System.currentTimeMillis() - delay;
    message.setReEnqueuedAt(time);
    assertEquals(message.getReEnqueuedAt(), time);
  }

  @Test
  public void testSetAccessTime() throws JsonProcessingException {
    RqueueMessage message = new RqueueMessage(queueName, queueMessage, retryCount, delay);
    Long time = System.currentTimeMillis() - delay;
    message.setAccessTime(time);
    assertEquals(message.getAccessTime(), time);
  }

  @Test
  public void testObjectEquality() throws JsonProcessingException {
    RqueueMessage message = new RqueueMessage(queueName, queueMessage, retryCount, delay);
    String stringMessage = objectMapper.writeValueAsString(message);
    assertEquals(message, objectMapper.readValue(stringMessage, RqueueMessage.class));
  }

  @Test
  public void testObjectEqualityWithoutDelay() throws JsonProcessingException {
    RqueueMessage message = new RqueueMessage(queueName, queueMessage, retryCount, null);
    String stringMessage = objectMapper.writeValueAsString(message);
    assertEquals(message, objectMapper.readValue(stringMessage, RqueueMessage.class));
  }

  @Test
  public void testObjectEqualityWithDifferentObject() throws JsonProcessingException {
    RqueueMessage message = new RqueueMessage(queueName, queueMessage, retryCount, delay);
    assertNotEquals(message, new Object());
  }

  @Test
  public void testObjectEqualityWithDifferentId() throws JsonProcessingException {
    RqueueMessage message = new RqueueMessage(queueName, queueMessage, retryCount, delay);
    RqueueMessage message2 = new RqueueMessage(queueName + "2", queueMessage, retryCount, delay);
    assertNotEquals(message, message2);
  }

  @Test
  public void testObjectEqualityWithDifferentMessageContent() throws JsonProcessingException {
    RqueueMessage message = new RqueueMessage(queueName, queueMessage + 1, retryCount, delay);
    RqueueMessage message2 = new RqueueMessage(queueName, queueMessage + 2, retryCount, delay);
    assertNotEquals(message, message2);
  }

  @Test
  public void testToString() throws JsonProcessingException {
    RqueueMessage message = new RqueueMessage(queueName, queueMessage, retryCount, delay);
    String toString =
        "RqueueMessage(id="
            + message.getId()
            + ", queueName=test-queue, message=This is a test message, retryCount="
            + retryCount
            + ", queuedTime="
            + message.getQueuedTime()
            + ", processAt="
            + message.getProcessAt()
            + ", accessTime=null, reEnqueuedAt=null)";
    assertEquals(toString, message.toString());
  }
}
