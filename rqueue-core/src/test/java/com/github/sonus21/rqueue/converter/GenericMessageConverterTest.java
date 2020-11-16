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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import com.github.sonus21.rqueue.annotation.MessageGenericField;
import com.github.sonus21.rqueue.listener.RqueueMessageHeaders;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.UUID;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.messaging.Message;
import org.springframework.messaging.support.GenericMessage;

@ExtendWith(MockitoExtension.class)
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

  @Test
  public void toMessageSet() {
    assertNull(
        genericMessageConverter.toMessage(
            Collections.singleton("Foo"), RqueueMessageHeaders.emptyMessageHeaders()));
  }

  @Test
  public void toMessageEmptyList() {
    assertNull(
        genericMessageConverter.toMessage(
            Collections.emptyList(), RqueueMessageHeaders.emptyMessageHeaders()));
  }

  @Test
  public void testMessageNonEmptyList() {
    assertNull(
        genericMessageConverter.toMessage(
            Collections.emptyList(), RqueueMessageHeaders.emptyMessageHeaders()));
  }

  @Test
  public void testToAndFromMessageList() {
    List<TestData> dataList = Arrays.asList(testData);
    Message<?> message =
        genericMessageConverter.toMessage(dataList, RqueueMessageHeaders.emptyMessageHeaders());
    List<TestData> fromMessage =
        (List<TestData>) genericMessageConverter.fromMessage(message, null);
    assertEquals(dataList, fromMessage);
  }

  @Test
  public void testGenericMessageToAndFrom() {
    GenericTestData<TestData> data = new GenericTestData<>(10, testData);
    Message<?> message =
        genericMessageConverter.toMessage(data, RqueueMessageHeaders.emptyMessageHeaders());
    GenericTestData<TestData> fromMessage =
        (GenericTestData<TestData>) genericMessageConverter.fromMessage(message, null);
    assertEquals(data, fromMessage);
  }

  @Test
  public void testMultipleGenericFieldMessageToAndFrom() {
    MultiGenericTestData<String, Integer> data = new MultiGenericTestData<>("Test", 10, testData);
    Message<?> message =
        genericMessageConverter.toMessage(data, RqueueMessageHeaders.emptyMessageHeaders());
    MultiGenericTestData<String, Integer> fromMessage =
        (MultiGenericTestData<String, Integer>) genericMessageConverter.fromMessage(message, null);
    assertEquals(data, fromMessage);
  }

  @Test
  public void testMultiLevelGenericMessageToAndFrom() {
    GenericTestData<String> testData = new GenericTestData<>(10, "foo");
    GenericTestData<Integer> testData2 = new GenericTestData<>(100, 200);
    MultiLevelGenericTestData<String, Integer> data =
        new MultiLevelGenericTestData<>("test", testData, testData2);
    Message<?> message =
        genericMessageConverter.toMessage(data, RqueueMessageHeaders.emptyMessageHeaders());
    MultiLevelGenericTestData<String, Integer> fromMessage =
        (MultiLevelGenericTestData<String, Integer>)
            genericMessageConverter.fromMessage(message, null);
    assertEquals(data, fromMessage);
  }

  @Test
  public void testMultiLevelGenericMessageToAndFromWithoutAllArgsConstructor() {
    GenericTestData<String> testData = new GenericTestData<>(10, "foo");
    GenericTestData<Integer> testData2 = new GenericTestData<>(100, 200);
    MultiLevelGenericTestDataNoArgs<String, Integer> data = new MultiLevelGenericTestDataNoArgs<>();
    data.setData("Test");
    data.setTGenericTestData(testData);
    data.setVGenericTestData(testData2);
    Message<?> message =
        genericMessageConverter.toMessage(data, RqueueMessageHeaders.emptyMessageHeaders());
    MultiLevelGenericTestDataNoArgs<String, Integer> fromMessage =
        (MultiLevelGenericTestDataNoArgs<String, Integer>)
            genericMessageConverter.fromMessage(message, null);
    assertEquals(data, fromMessage);
  }

  @Data
  @NoArgsConstructor
  public static class MultiLevelGenericTestDataNoArgs<T, V> {
    private String data;
    @MessageGenericField private GenericTestData<T> tGenericTestData;
    @MessageGenericField private GenericTestData<V> vGenericTestData;
  }

  @Data
  @AllArgsConstructor
  @NoArgsConstructor
  public static class MultiLevelGenericTestData<T, V> {
    private String data;
    @MessageGenericField private GenericTestData<T> tGenericTestData;
    @MessageGenericField private GenericTestData<V> vGenericTestData;
  }

  @Data
  @AllArgsConstructor
  @NoArgsConstructor
  public static class MultiGenericTestData<K, V> {
    @MessageGenericField private K key;
    @MessageGenericField private V value;
    private TestData testData;
  }

  @Data
  @AllArgsConstructor
  @NoArgsConstructor
  public static class GenericTestData<T> {
    private Integer index;
    @MessageGenericField private T data;
  }

  @Data
  @AllArgsConstructor
  @NoArgsConstructor
  public static class TestData {
    private String id;
    private String message;
  }
}
