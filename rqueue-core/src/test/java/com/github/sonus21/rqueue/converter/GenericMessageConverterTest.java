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

import com.github.sonus21.rqueue.listener.RqueueMessageHeaders;
import java.lang.reflect.GenericArrayType;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.lang.reflect.TypeVariable;
import java.lang.reflect.WildcardType;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.UUID;
import java.util.stream.Collectors;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.messaging.Message;
import org.springframework.messaging.support.GenericMessage;

@ExtendWith(MockitoExtension.class)
@SuppressWarnings("unchecked")
public class GenericMessageConverterTest {
  private static final GenericMessageConverter genericMessageConverter =
      new GenericMessageConverter();
  private static Comment comment = new Comment(UUID.randomUUID().toString(), "This is test");
  private static Email email = new Email(UUID.randomUUID().toString(), "This is test");

  @Test
  public void fromMessageIoException() {
    Message<String> message = new GenericMessage<>("dasasd");
    assertNull(genericMessageConverter.fromMessage(message, null));
  }

  @Test
  public void fromMessageClassCastException() {
    Message<Comment> commentMessage = new GenericMessage<>(comment);
    assertNull(genericMessageConverter.fromMessage(commentMessage, null));
  }

  @Test
  public void fromMessageClassNotFoundException() {
    Message<String> message = (Message<String>) genericMessageConverter.toMessage(comment, null);
    String payload = Objects.requireNonNull(message).getPayload().replace("Comment", "SomeData");
    Message<String> updatedMessage = new GenericMessage<>(payload);
    assertNull(genericMessageConverter.fromMessage(updatedMessage, null));
  }

  @Test
  public void toMessageEmptyObject() {
    Message<String> m = (Message<String>) genericMessageConverter.toMessage(new Object(), null);
    assertNull(m);
  }

  @Test
  public void toMessage() {
    Message<String> m = (Message<String>) genericMessageConverter.toMessage(comment, null);
    Comment comment1 = (Comment) genericMessageConverter.fromMessage(m, null);
    assertEquals(comment1, comment);
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
    List<Comment> dataList = Collections.singletonList(comment);
    Message message =
        genericMessageConverter.toMessage(dataList, RqueueMessageHeaders.emptyMessageHeaders());
    List<Comment> fromMessage = (List<Comment>) genericMessageConverter.fromMessage(message, null);
    assertEquals(dataList, fromMessage);
  }

  @Test
  public void testGenericMessageToReturnNull() {
    GenericTestData<Comment> data = new GenericTestData<>(10, comment);
    Message message =
        genericMessageConverter.toMessage(data, RqueueMessageHeaders.emptyMessageHeaders());
    assertNull(message);
  }

  @Test
  @Disabled
  public void testMultipleGenericFieldMessageToAndFrom() {
    MultiGenericTestData<Comment, Email> data = new MultiGenericTestData<>(10, comment, email);
    Message message =
        genericMessageConverter.toMessage(data, RqueueMessageHeaders.emptyMessageHeaders());
    MultiGenericTestData<Comment, Email> fromMessage =
        (MultiGenericTestData<Comment, Email>) genericMessageConverter.fromMessage(message, null);
    assertEquals(data, fromMessage);
  }

  @Test
  @Disabled
  public void testMultipleGenericSameTypeMessageToAndFrom() {
    MultiGenericTestData<Comment, Comment> data = new MultiGenericTestData<>(10, comment, comment);
    Message message =
        genericMessageConverter.toMessage(data, RqueueMessageHeaders.emptyMessageHeaders());
    MultiGenericTestData<Comment, Comment> fromMessage =
        (MultiGenericTestData<Comment, Comment>) genericMessageConverter.fromMessage(message, null);
    assertEquals(data, fromMessage);
  }

  @Test
  @Disabled
  public void testMultiLevelGenericMessageToAndFrom() {
    GenericTestData<Comment> testData = new GenericTestData<>(10, comment);
    GenericTestData<Email> testData2 = new GenericTestData<>(100, email);
    MultiLevelGenericTestData<Comment, Email> data =
        new MultiLevelGenericTestData<>("test", testData, testData2);
    Message message =
        genericMessageConverter.toMessage(data, RqueueMessageHeaders.emptyMessageHeaders());
    MultiLevelGenericTestData<Comment, Email> fromMessage =
        (MultiLevelGenericTestData<Comment, Email>)
            genericMessageConverter.fromMessage(message, null);
    assertEquals(data, fromMessage);
  }

  @Test
  @Disabled
  public void testMultiLevelGenericMessageToAndFromWithoutAllArgsConstructor() {
    GenericTestData<Comment> testData = new GenericTestData<>(10, comment);
    GenericTestData<Email> testData2 = new GenericTestData<>(100, email);
    MultiLevelGenericTestDataNoArgs<Comment, Email> data = new MultiLevelGenericTestDataNoArgs<>();
    data.setData("Test");
    data.setTGenericTestData(testData);
    data.setVGenericTestData(testData2);
    Message message =
        genericMessageConverter.toMessage(data, RqueueMessageHeaders.emptyMessageHeaders());
    MultiLevelGenericTestDataNoArgs<Comment, Email> fromMessage =
        (MultiLevelGenericTestDataNoArgs<Comment, Email>)
            genericMessageConverter.fromMessage(message, null);
    assertEquals(data, fromMessage);
  }

  @Test
  @Disabled
  public void testPredefinedGenericTypeToFromMessage() {
    MultiGenericTestData<Comment, Email> multiGenericTestData =
        new MultiGenericTestData<>(10, comment, email);
    GenericTestDataWithPredefinedType data =
        new GenericTestDataWithPredefinedType(200, multiGenericTestData);
    Message message =
        genericMessageConverter.toMessage(data, RqueueMessageHeaders.emptyMessageHeaders());
    GenericTestDataWithPredefinedType fromMessage =
        (GenericTestDataWithPredefinedType) genericMessageConverter.fromMessage(message, null);
    assertEquals(data, fromMessage);
  }

  @Test
  @Disabled
  public void testMultiGenericSameTypeToFromMessage() {
    GenericTestData<Comment> genericTestData = new GenericTestData<>(100, comment);
    MultiGenericTestData<Comment, Comment> multiGenericTestData =
        new MultiGenericTestData<>(200, comment, comment);
    MultiGenericTestDataSameType<Comment> data =
        new MultiGenericTestDataSameType<>(10, genericTestData, multiGenericTestData);
    Message message =
        genericMessageConverter.toMessage(data, RqueueMessageHeaders.emptyMessageHeaders());
    MultiGenericTestDataSameType<Comment> fromMessage =
        (MultiGenericTestDataSameType<Comment>) genericMessageConverter.fromMessage(message, null);
    assertEquals(data, fromMessage);
  }

  @Test
  @Disabled
  public void testMultiLevelGenericTestDataFixedTypeToFromMessage() {
    GenericTestData<Comment> genericTestData = new GenericTestData<>(100, comment);
    MultiGenericTestData<Email, String> multiGenericTestData =
        new MultiGenericTestData<>(200, email, "comment");
    MultiLevelGenericTestDataFixedType<Comment, Email> data =
        new MultiLevelGenericTestDataFixedType<>("10", genericTestData, multiGenericTestData);
    Message message =
        genericMessageConverter.toMessage(data, RqueueMessageHeaders.emptyMessageHeaders());
    MultiLevelGenericTestDataFixedType<Comment, Email> fromMessage =
        (MultiLevelGenericTestDataFixedType<Comment, Email>)
            genericMessageConverter.fromMessage(message, null);
    assertEquals(data, fromMessage);
  }

  @Data
  @NoArgsConstructor
  public static class MultiLevelGenericTestDataNoArgs<T, V> {
    private String data;
    private GenericTestData<T> tGenericTestData;
    private GenericTestData<V> vGenericTestData;
  }

  @Data
  @AllArgsConstructor
  @NoArgsConstructor
  public static class MultiLevelGenericTestData<T, V> {
    private String data;
    private GenericTestData<T> tGenericTestData;
    private GenericTestData<V> vGenericTestData;
  }

  @Data
  @AllArgsConstructor
  @NoArgsConstructor
  public static class MultiLevelGenericTestDataFixedType<T, V> {
    private String data;
    private GenericTestData<T> tGenericTestData;
    private MultiGenericTestData<V, String> vGenericTestData;
  }

  @Data
  @AllArgsConstructor
  @NoArgsConstructor
  public static class MultiGenericTestData<K, V> {
    private Integer index;
    private K key;
    private V value;
  }

  @Data
  @AllArgsConstructor
  @NoArgsConstructor
  public static class GenericTestData<T> {
    private Integer index;
    private T data;
  }

  @Data
  @AllArgsConstructor
  @NoArgsConstructor
  public static class Comment {
    private String id;
    private String message;
  }

  @Data
  @AllArgsConstructor
  @NoArgsConstructor
  public static class Email {
    private String id;
    private String subject;
  }

  // https://stackoverflow.com/questions/64873444/generic-class-type-parameter-detail-at-runtime
  static class MappingRegistrar<T> {

    private final Type type;

    protected MappingRegistrar() {
      Class<?> cls = getClass();
      Type[] type = ((ParameterizedType) cls.getGenericSuperclass()).getActualTypeArguments();
      this.type = type[0];
    }

    public void seeIt() {
      innerSeeIt(type);
    }

    private void innerSeeIt(Type type) {
      if (type instanceof Class) {
        Class<?> cls = (Class<?>) type;
        boolean isArray = cls.isArray();
        if (isArray) {
          System.out.print(cls.getComponentType().getSimpleName() + "[]");
          return;
        }
        System.out.print(cls.getSimpleName());
      }

      if (type instanceof TypeVariable) {
        Type[] bounds = ((TypeVariable<?>) type).getBounds();
        String s =
            Arrays.stream(bounds)
                .map(Type::getTypeName)
                .collect(Collectors.joining(", ", "[", "]"));
        System.out.print(s);
      }

      if (type instanceof ParameterizedType) {
        ParameterizedType parameterizedType = (ParameterizedType) type;
        String rawType = parameterizedType.getRawType().getTypeName();
        System.out.print(rawType + "<");
        Type[] arguments = parameterizedType.getActualTypeArguments();

        for (int i = 0; i < arguments.length; ++i) {
          innerSeeIt(arguments[i]);
          if (i != arguments.length - 1) {
            System.out.print(", ");
          }
        }

        System.out.print(">");
        // System.out.println(Arrays.toString(arguments));
      }

      if (type instanceof GenericArrayType) {
        // you need to handle this one too
      }

      if (type instanceof WildcardType) {
        // you need to handle this one too, but it isn't trivial
      }
    }
  }

  @Data
  @AllArgsConstructor
  @NoArgsConstructor
  public static class MultiGenericTestDataSameType<T> extends MappingRegistrar {
    private Integer index;
    private GenericTestData<T> genericTestData;
    private MultiGenericTestData<T, T> multiGenericTestData;
  }

  @Data
  @NoArgsConstructor
  @AllArgsConstructor
  public static class GenericTestDataWithPredefinedType {
    private Integer index;
    private MultiGenericTestData<Comment, Email> data;
  }

  @Test
  @Disabled
  public void foo() {
    MappingRegistrar<?> m = new MappingRegistrar<MultiGenericTestDataSameType<String>>() {};
    m.seeIt();
    MultiGenericTestDataSameType<String> m2 = new MultiGenericTestDataSameType<>();
    m2.seeIt();
  }
}
