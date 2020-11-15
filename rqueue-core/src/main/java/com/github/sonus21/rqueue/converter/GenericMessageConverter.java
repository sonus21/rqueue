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

import static org.springframework.util.Assert.notNull;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.sonus21.rqueue.annotation.MessageGenericField;
import com.github.sonus21.rqueue.utils.SerializationUtils;
import java.lang.reflect.Field;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageHeaders;
import org.springframework.messaging.converter.MessageConverter;
import org.springframework.messaging.support.GenericMessage;

/**
 * A converter to turn the payload of a {@link Message} from serialized form to a typed String and
 * vice versa.
 */
@Slf4j
public class GenericMessageConverter implements MessageConverter {
  private final ObjectMapper objectMapper;

  public GenericMessageConverter() {
    this.objectMapper = new ObjectMapper();
  }

  public GenericMessageConverter(ObjectMapper objectMapper) {
    notNull(objectMapper, "objectMapper cannot be null");
    this.objectMapper = objectMapper;
  }

  /**
   * Convert the payload of a {@link Message} from a serialized form to a typed Object of type
   * stored in message it self.
   *
   * <p>If the converter cannot perform the conversion it returns {@code null}.
   *
   * @param message the input message
   * @param targetClass the target class for the conversion
   * @return the result of the conversion, or {@code null} if the converter cannot perform the
   *     conversion.
   */
  @Override
  public Object fromMessage(Message<?> message, Class<?> targetClass) {
    try {
      String payload = (String) message.getPayload();
      if (SerializationUtils.isJson(payload)) {
        Msg msg = objectMapper.readValue(payload, Msg.class);
        String[] classNames = splitClassNames(msg.getName());
        if (classNames.length == 1) {
          Class<?> c = Thread.currentThread().getContextClassLoader().loadClass(msg.getName());
          return objectMapper.readValue(msg.msg, c);
        }
        Class<?> envelopeClass =
            Thread.currentThread().getContextClassLoader().loadClass(classNames[0]);
        Class<?>[] classes = new Class<?>[classNames.length - 1];
        for (int i = 1; i < classNames.length; i++) {
          classes[i - 1] = Thread.currentThread().getContextClassLoader().loadClass(classNames[i]);
        }
        JavaType type =
            objectMapper.getTypeFactory().constructParametricType(envelopeClass, classes);
        return objectMapper.readValue(msg.msg, type);
      }
    } catch (Exception e) {
      log.warn("Deserialization of message {} failed", message, e);
    }
    return null;
  }

  private String[] splitClassNames(String name) {
    return name.split("#");
  }

  private String getClassNameForCollection(String name, Collection<?> payload) {
    if (payload instanceof List) {
      if (payload.isEmpty()) {
        return null;
      }
      return name + '#' + ((List<?>) payload).get(0).getClass().getName();
    }
    return null;
  }

  private String getGenericFieldBasedClassName(String name, Object payload) {
    List<String> genericFieldClassNames = new LinkedList<>();
    for (Field field : payload.getClass().getDeclaredFields()) {
      if (field.isAnnotationPresent(MessageGenericField.class)) {
        try {
          Object fieldVal = field.get(payload);
          genericFieldClassNames.add(fieldVal.getClass().getName());
        } catch (IllegalAccessException e) {
          log.error("Field can not be read", e);
          return null;
        }
      }
    }
    if (genericFieldClassNames.isEmpty()) {
      return name;
    }
    return name + '#' + String.join("#", genericFieldClassNames);
  }

  private String getClassName(Object payload) {
    String name = payload.getClass().getName();
    if (payload instanceof Collection) {
      return getClassNameForCollection(name, (Collection<?>) payload);
    }
    return getGenericFieldBasedClassName(name, payload);
  }

  /**
   * Create a {@link Message} whose payload is the result of converting the given payload Object to
   * serialized form. It ignores all headers components.
   *
   * <p>If the converter cannot perform the conversion, it return {@code null}.
   *
   * @param payload the Object to convert
   * @param headers optional headers for the message (may be {@code null})
   * @return the new message, or {@code null}
   */
  @Override
  public Message<?> toMessage(Object payload, MessageHeaders headers) {
    String name = getClassName(payload);
    if (name == null) {
      return null;
    }
    try {
      String msg = objectMapper.writeValueAsString(payload);
      Msg message = new Msg(msg, name);
      return new GenericMessage<>(objectMapper.writeValueAsString(message));
    } catch (JsonProcessingException e) {
      log.error("Serialisation failed", e);
      return null;
    }
  }

  @Getter
  @Setter
  @NoArgsConstructor
  @AllArgsConstructor
  private static class Msg {
    private String msg;
    private String name;
  }
}
