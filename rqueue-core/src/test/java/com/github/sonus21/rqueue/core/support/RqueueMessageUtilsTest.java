/*
 * Copyright (c) 2020-2023 Sonu Kumar
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

package com.github.sonus21.rqueue.core.support;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import com.github.sonus21.TestBase;
import com.github.sonus21.rqueue.CoreUnitTest;
import com.github.sonus21.rqueue.converter.GenericMessageConverter;
import com.github.sonus21.rqueue.core.DefaultRqueueMessageConverter;
import com.github.sonus21.rqueue.core.RqueueMessage;
import com.github.sonus21.rqueue.listener.RqueueMessageHeaders;
import com.google.common.collect.ImmutableList;
import java.util.UUID;
import lombok.Data;
import org.apache.commons.lang3.RandomStringUtils;
import org.junit.jupiter.api.Test;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageHeaders;
import org.springframework.messaging.converter.MessageConversionException;
import org.springframework.messaging.converter.MessageConverter;
import org.springframework.messaging.converter.StringMessageConverter;
import org.springframework.messaging.support.GenericMessage;

@CoreUnitTest
class RqueueMessageUtilsTest extends TestBase {

  private final String queue = "test-queue";
  DefaultRqueueMessageConverter messageConverter = new DefaultRqueueMessageConverter();
  DefaultRqueueMessageConverter messageConverter2 =
      new DefaultRqueueMessageConverter(
          ImmutableList.of(
              new GenericMessageConverter(),
              new StringMessageConverter(),
              new NoMessageConverter()));

  @Test
  void buildPeriodicMessage() {
    Email email = Email.newInstance();
    long startTime = System.currentTimeMillis();
    RqueueMessage message =
        RqueueMessageUtils.buildPeriodicMessage(
            messageConverter,
            queue,
            null,
            email,
            null,
            10_000L,
            RqueueMessageHeaders.emptyMessageHeaders());
    assertEquals(10_000L, message.getPeriod());
    long now = System.currentTimeMillis();
    assertTrue(
        message.getProcessAt() >= startTime + 10_000L && message.getProcessAt() <= now + 10_000L);
    assertEquals(queue, message.getQueueName());
    assertEquals(0, message.getQueuedTime());
    assertNotNull(message.getId());
    assertNotNull(message.getMessage());
    String convertedMessage =
        (String)
            messageConverter
                .toMessage(email, RqueueMessageHeaders.emptyMessageHeaders(), null)
                .getPayload();
    assertEquals(convertedMessage, message.getMessage());
  }

  @Test
  void buildMessage() {
    Email email = Email.newInstance();
    long startTime = System.currentTimeMillis();
    long startTimeInNano = System.nanoTime();
    RqueueMessage message =
        RqueueMessageUtils.buildMessage(
            messageConverter,
            queue,
            null,
            email,
            null,
            null,
            RqueueMessageHeaders.emptyMessageHeaders());
    assertEquals(0, message.getPeriod());
    long now = System.currentTimeMillis();
    long nowNano = System.nanoTime();
    assertTrue(message.getProcessAt() >= startTime && message.getProcessAt() <= now);
    assertTrue(message.getQueuedTime() >= startTimeInNano && message.getQueuedTime() <= nowNano);
    assertEquals(queue, message.getQueueName());
    assertNotNull(message.getId());
    assertNotNull(message.getMessage());
    assertNull(message.getRetryCount());
    assertEquals(0, message.getFailureCount());
    String convertedMessage =
        (String)
            messageConverter
                .toMessage(email, RqueueMessageHeaders.emptyMessageHeaders(), null)
                .getPayload();
    assertEquals(convertedMessage, message.getMessage());
  }

  @Test
  void buildMessageWithDelay() {
    Email email = Email.newInstance();
    long startTime = System.currentTimeMillis();
    long startTimeInNano = System.nanoTime();
    RqueueMessage message =
        RqueueMessageUtils.buildMessage(
            messageConverter,
            queue,
            null,
            email,
            3,
            10_000L,
            RqueueMessageHeaders.emptyMessageHeaders());
    assertEquals(0, message.getPeriod());
    long now = System.currentTimeMillis();
    long nowNano = System.nanoTime();
    assertTrue(
        message.getProcessAt() >= startTime + 10_000L && message.getProcessAt() <= now + 10_000L);
    assertTrue(message.getQueuedTime() >= startTimeInNano && message.getQueuedTime() <= nowNano);
    assertEquals(queue, message.getQueueName());
    assertNotNull(message.getId());
    assertNotNull(message.getMessage());
    assertEquals(3, message.getRetryCount());
    assertEquals(0, message.getFailureCount());
    String convertedMessage =
        (String)
            messageConverter
                .toMessage(email, RqueueMessageHeaders.emptyMessageHeaders(), null)
                .getPayload();
    assertEquals(convertedMessage, message.getMessage());
  }

  @Test
  void buildMessageNull() {
    GenericClass<String> genericClass = new GenericClass<>();
    genericClass.id = UUID.randomUUID().toString();
    try {
      RqueueMessageUtils.buildMessage(
          messageConverter,
          queue,
          null,
          genericClass,
          3,
          10_000L,
          RqueueMessageHeaders.emptyMessageHeaders());
      fail("message conversion should fail");
    } catch (MessageConversionException e) {
      assertEquals("Message could not be build (null)", e.getMessage());
    }
  }

  @Test
  void buildPeriodicMessageNull() {
    GenericClass<String> genericClass = new GenericClass<>();
    genericClass.id = UUID.randomUUID().toString();
    try {
      RqueueMessageUtils.buildPeriodicMessage(
          messageConverter,
          queue,
          null,
          genericClass,
          null,
          10_000L,
          RqueueMessageHeaders.emptyMessageHeaders());
      fail("message conversion should fail");
    } catch (MessageConversionException e) {
      assertEquals("Message could not be build (null)", e.getMessage());
    }
  }

  @Test
  void buildMessageReturnInvalidType() {
    GenericClass<String> genericClass = new GenericClass<>();
    genericClass.id = UUID.randomUUID().toString();
    try {
      RqueueMessageUtils.buildMessage(
          messageConverter2,
          queue,
          null,
          genericClass,
          3,
          10_000L,
          RqueueMessageHeaders.emptyMessageHeaders());
      fail("message conversion should fail");
    } catch (MessageConversionException e) {
      assertEquals("Message payload is neither String nor byte[]", e.getMessage());
    }
  }

  @Test
  void buildPeriodicMessageReturnInvalidType() {
    GenericClass<String> genericClass = new GenericClass<>();
    genericClass.id = UUID.randomUUID().toString();
    try {
      RqueueMessageUtils.buildPeriodicMessage(
          messageConverter2,
          queue,
          null,
          genericClass,
          null,
          10_000L,
          RqueueMessageHeaders.emptyMessageHeaders());
      fail("message conversion should fail");
    } catch (MessageConversionException e) {
      assertEquals("Message payload is neither String nor byte[]", e.getMessage());
    }
  }

  static class NoMessageConverter implements MessageConverter {

    @Override
    public Object fromMessage(Message<?> message, Class<?> aClass) {
      return null;
    }

    @Override
    public Message<?> toMessage(Object o, MessageHeaders messageHeaders) {
      return new GenericMessage<>(o);
    }
  }

  @Data
  static class Email {

    String id;
    String email;

    static Email newInstance() {
      Email email = new Email();
      email.id = UUID.randomUUID().toString();
      email.email = RandomStringUtils.randomAlphabetic(10) + "@test.com";
      return email;
    }
  }

  @Data
  static class GenericClass<T> {

    T id;
  }
}
