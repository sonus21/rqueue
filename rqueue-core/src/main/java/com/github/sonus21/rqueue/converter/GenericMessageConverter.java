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

package com.github.sonus21.rqueue.converter;

import static org.springframework.util.Assert.notNull;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.sonus21.rqueue.utils.SerializationUtils;
import java.lang.reflect.TypeVariable;
import java.util.Collection;
import java.util.List;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageHeaders;
import org.springframework.messaging.converter.SmartMessageConverter;
import org.springframework.messaging.support.GenericMessage;

/**
 * A converter to turn the payload of a {@link Message} from serialized form to a typed String and
 * vice versa. This class does not support generic class except {@link List},even for list the
 * entries should be non generic.
 */
@Slf4j
public class GenericMessageConverter implements SmartMessageConverter {

  private final SmartMessageSerDes smartMessageSerDes;

  public GenericMessageConverter() {
    ObjectMapper mapper = SerializationUtils.createObjectMapper();
    this.smartMessageSerDes = new SmartMessageSerDes(mapper);
  }

  public GenericMessageConverter(ObjectMapper objectMapper) {
    notNull(objectMapper, "objectMapper cannot be null");
    this.smartMessageSerDes = new SmartMessageSerDes(objectMapper);
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
    log.trace("Payload: {} headers: {}", payload, headers);
    String message = smartMessageSerDes.serialize(payload);
    if (message == null) {
      return null;
    }
    return new GenericMessage<>(message);
  }

  @Override
  public Object fromMessage(Message<?> message, Class<?> targetClass, Object conversionHint) {
    log.trace("Message: {} class: {} hint: {}", message, targetClass, conversionHint);
    return fromMessage(message, targetClass);
  }

  @Override
  public Message<?> toMessage(Object payload, MessageHeaders headers, Object conversionHint) {
    log.trace("Payload: {} headers: {} hint: {}", payload, headers, conversionHint);
    return toMessage(payload, headers);
  }

  /**
   * Convert the payload of a {@link Message} from a serialized form to a typed Object of type
   * stored in message it self.
   *
   * <p>If the converter cannot perform the conversion it returns {@code null}.
   *
   * @param message     the input message
   * @param targetClass the target class for the conversion
   * @return the result of the conversion, or {@code null} if the converter cannot perform the
   * conversion.
   */
  @Override
  public Object fromMessage(Message<?> message, Class<?> targetClass) {
    log.trace("Message: {} class: {}", message, targetClass);
    String payload;
    try {
      payload = (String) message.getPayload();
    } catch (ClassCastException e) {
      return null;
    }
    return smartMessageSerDes.deserialize(payload);
  }

  @Getter
  @Setter
  @NoArgsConstructor
  @AllArgsConstructor
  private static class Msg {

    private String msg;
    private String name;
  }

  public static class SmartMessageSerDes {

    private final ObjectMapper objectMapper;

    public SmartMessageSerDes(ObjectMapper objectMapper) {
      this.objectMapper = objectMapper;
    }

    private String[] splitClassNames(String name) {
      return name.split("#");
    }

    private String getClassNameForCollection(String name, Collection<?> payload) {
      if (payload instanceof List) {
        if (payload.isEmpty()) {
          return null;
        }
        String itemClassName = getClassName(((List<?>) payload).get(0));
        if (itemClassName == null) {
          return null;
        }
        return name + '#' + itemClassName;
      }
      return null;
    }

    private String getGenericFieldBasedClassName(Class<?> clazz) {
      TypeVariable<?>[] typeVariables = clazz.getTypeParameters();
      if (typeVariables.length == 0) {
        return clazz.getName();
      }
      return null;
    }

    private String getClassName(Object payload) {
      Class<?> payloadClass = payload.getClass();
      String name = payloadClass.getName();
      if (payload instanceof Collection) {
        return getClassNameForCollection(name, (Collection<?>) payload);
      }
      return getGenericFieldBasedClassName(payloadClass);
    }

    private JavaType getTargetType(Msg msg) throws ClassNotFoundException {
      String[] classNames = splitClassNames(msg.getName());
      if (classNames.length == 1) {
        Class<?> c = Thread.currentThread().getContextClassLoader().loadClass(msg.getName());
        return objectMapper.getTypeFactory().constructType(c);
      }
      Class<?> envelopeClass =
          Thread.currentThread().getContextClassLoader().loadClass(classNames[0]);
      Class<?>[] classes = new Class<?>[classNames.length - 1];
      for (int i = 1; i < classNames.length; i++) {
        classes[i - 1] = Thread.currentThread().getContextClassLoader().loadClass(classNames[i]);
      }
      return objectMapper.getTypeFactory().constructParametricType(envelopeClass, classes);
    }

    public Object deserialize(String payload) {
      try {
        if (SerializationUtils.isJson(payload)) {
          Msg msg = objectMapper.readValue(payload, Msg.class);
          JavaType type = getTargetType(msg);
          return objectMapper.readValue(msg.msg, type);
        }
      } catch (Exception e) {
        log.debug("Deserialization of message {} failed", payload, e);
      }
      return null;
    }

    public <T> T deserialize(byte[] payload, Class<T> clazz) {
      if (SerializationUtils.isEmpty(payload)) {
        return null;
      }
      try {
        return objectMapper.readValue(payload, clazz);
      } catch (Exception e) {
        log.debug("Deserialization of message {} failed", new String(payload), e);
      }
      return null;
    }

    public String serialize(Object payload) {
      String name = getClassName(payload);
      if (name == null) {
        return null;
      }
      try {
        String msg = objectMapper.writeValueAsString(payload);
        Msg message = new Msg(msg, name);
        return objectMapper.writeValueAsString(message);
      } catch (JsonProcessingException e) {
        log.debug("Serialisation failed", e);
        return null;
      }
    }
  }
}
