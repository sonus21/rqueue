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

package com.github.sonus21.rqueue.converter;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.sonus21.TestBase;
import com.github.sonus21.rqueue.CoreUnitTest;
import java.util.UUID;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;
import org.apache.commons.lang3.RandomStringUtils;
import org.junit.jupiter.api.Test;
import org.springframework.messaging.Message;
import org.springframework.messaging.converter.MessageConverter;

@CoreUnitTest
class JsonMessageConverterTest extends TestBase {

  private final ObjectMapper objectMapper = new ObjectMapper();
  private final MessageConverter messageConverter = new JsonMessageConverter();
  private final MessageConverter messageConverter2 = new JsonMessageConverter(objectMapper);

  @Test
  void fromMessage() {
    TestData testData = TestData.getInstance();
    TestDataX testDataX = TestDataX.getInstance();
    Message<?> testDataMessage = messageConverter.toMessage(testData, null);
    Message<?> testDataXMessage = messageConverter.toMessage(testDataX, null);
    assertNull(messageConverter.fromMessage(testDataMessage, null));
    assertNull(messageConverter.fromMessage(testDataXMessage, null));

    //  simple serialization
    assertEquals(testData, messageConverter.fromMessage(testDataMessage, TestData.class));
    assertEquals(testDataX, messageConverter.fromMessage(testDataXMessage, TestDataX.class));

    // unknown property error
    assertNull(messageConverter2.fromMessage(testDataXMessage, TestData.class));

    // serializing from type 1 to type 2
    TestDataX newTestDataX =
        (TestDataX) messageConverter2.fromMessage(testDataMessage, TestDataX.class);
    assertEquals(testData.id, newTestDataX.id);
    assertEquals(testData.message, newTestDataX.message);
    assertNull(newTestDataX.x);
  }

  @Test
  void toMessage() {
    assertNotNull(messageConverter.toMessage(TestData.getInstance(), null));
    assertNotNull(messageConverter.toMessage(TestDataX.getInstance(), null));
    assertNotNull(messageConverter2.toMessage(TestData.getInstance(), null));
    assertNotNull(messageConverter2.toMessage(TestDataX.getInstance(), null));
  }

  @Data
  @AllArgsConstructor
  @NoArgsConstructor
  @EqualsAndHashCode
  public static class TestData {

    private String id;
    private String message;

    public static TestData getInstance() {
      return new TestData(UUID.randomUUID().toString(), RandomStringUtils.randomAlphabetic(100));
    }
  }

  @Data
  @AllArgsConstructor
  @NoArgsConstructor
  @EqualsAndHashCode
  public static class TestDataX {

    private String id;
    private String message;
    private String x;

    public static TestDataX getInstance() {
      return new TestDataX(
          UUID.randomUUID().toString(),
          RandomStringUtils.randomAlphabetic(100),
          RandomStringUtils.randomAlphabetic(100));
    }
  }
}
