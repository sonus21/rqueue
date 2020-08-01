/*
 * Copyright 2020 Sonu Kumar
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.github.sonus21.rqueue.converter;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import java.util.Objects;
import java.util.UUID;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;
import org.springframework.messaging.Message;
import org.springframework.messaging.support.GenericMessage;

@RunWith(MockitoJUnitRunner.StrictStubs.class)
public class GenericMessageConverterTest {
  private GenericMessageConverter genericMessageConverter = new GenericMessageConverter();
  private TestData testData = new TestData(UUID.randomUUID().toString(), "This is test");

  @Test
  public void fromMessageIoException() {
    Message<String> message = new GenericMessage<>("dasasd");
    assertNull(genericMessageConverter.fromMessage(message, null));
  }

  @Test
  public void fromMessageClassCastException() {
    Message<TestData> message1 = new GenericMessage<>(testData);
    assertNull(genericMessageConverter.fromMessage(message1, null));
  }

  @Test
  public void fromMessageClassNotFoundException() {
    Message<String> message2 = (Message<String>) genericMessageConverter.toMessage(testData, null);
    String payload = Objects.requireNonNull(message2).getPayload().replace("TestData", "SomeData");
    Message<String> message3 = new GenericMessage<>(payload);
    assertNull(genericMessageConverter.fromMessage(message3, null));
  }

  @Test
  public void toMessageEmptyObject() {
    Message<String> m = (Message<String>) genericMessageConverter.toMessage(new Object(), null);
    assertNull(m);
  }

  @Test
  public void toMessage() {
    Message<String> m = (Message<String>) genericMessageConverter.toMessage(testData, null);
    TestData t2 = (TestData) genericMessageConverter.fromMessage(m, null);
    assertEquals(testData, t2);
  }

  @Data
  @AllArgsConstructor
  @NoArgsConstructor
  private static class TestData {
    private String id;
    private String message;
  }
}
