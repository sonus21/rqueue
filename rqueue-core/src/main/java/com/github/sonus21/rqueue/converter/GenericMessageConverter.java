/*
 * Copyright (c) 2019-2026 Sonu Kumar
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

import com.github.sonus21.rqueue.serdes.RqJacksonSerDes;
import com.github.sonus21.rqueue.serdes.RqJacksonTypeFactory;
import com.github.sonus21.rqueue.serdes.RqueueSerDes;
import com.github.sonus21.rqueue.serdes.RqueueTypeFactory;
import com.github.sonus21.rqueue.serdes.SerializationUtils;
import com.github.sonus21.rqueue.serdes.TypeEnvelop;
import java.lang.reflect.Field;
import java.lang.reflect.TypeVariable;
import java.nio.charset.StandardCharsets;
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
import tools.jackson.core.JacksonException;
import tools.jackson.core.JsonGenerator;
import tools.jackson.core.JsonParser;
import tools.jackson.databind.DeserializationContext;
import tools.jackson.databind.ObjectMapper;
import tools.jackson.databind.SerializationContext;
import tools.jackson.databind.annotation.JsonDeserialize;
import tools.jackson.databind.annotation.JsonSerialize;
import tools.jackson.databind.deser.std.StdDeserializer;
import tools.jackson.databind.ser.std.StdSerializer;

/**
 * A converter to turn the payload of a {@link Message} from serialized form to a typed String and
 * vice versa. Supports {@link List} and single-level generic envelope types (e.g. {@code Event<T>})
 * where type parameters are non-generic and can be resolved from non-null field values.
 */
@Slf4j
public class GenericMessageConverter implements SmartMessageConverter {

  private final SmartMessageSerDes smartMessageSerDes;

  public GenericMessageConverter() {
    this.smartMessageSerDes =
        new SmartMessageSerDes(SerializationUtils.getSerDes(), SerializationUtils.getTypeFactory());
  }

  public GenericMessageConverter(ObjectMapper objectMapper) {
    notNull(objectMapper, "objectMapper cannot be null");
    this.smartMessageSerDes = new SmartMessageSerDes(new RqJacksonSerDes(objectMapper),
        new RqJacksonTypeFactory(objectMapper));
  }

  public GenericMessageConverter(RqueueSerDes serDes, RqueueTypeFactory typeFactory) {
    notNull(serDes, "serDes cannot be null");
    notNull(typeFactory, "typeFactory cannot be null");
    this.smartMessageSerDes = new SmartMessageSerDes(serDes, typeFactory);
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

    @JsonSerialize(using = Utf8BytesSerializer.class)
    @JsonDeserialize(using = Utf8BytesDeserializer.class)
    private byte[] msg;

    private String name;
  }

  private static class Utf8BytesSerializer extends StdSerializer<byte[]> {

    Utf8BytesSerializer() {
      super(byte[].class);
    }

    @Override
    public void serialize(byte[] value, JsonGenerator gen, SerializationContext ctx)
        throws JacksonException {
      gen.writeString(new String(value, StandardCharsets.UTF_8));
    }
  }

  private static class Utf8BytesDeserializer extends StdDeserializer<byte[]> {

    Utf8BytesDeserializer() {
      super(byte[].class);
    }

    @Override
    public byte[] deserialize(JsonParser p, DeserializationContext ctx) throws JacksonException {
      String text = p.getString();
      if (text == null) {
        return null;
      }
      return text.getBytes(StandardCharsets.UTF_8);
    }
  }

  public static class SmartMessageSerDes {

    private final RqueueSerDes serDes;
    private final RqueueTypeFactory typeFactory;

    public SmartMessageSerDes(RqueueSerDes serDes, RqueueTypeFactory typeFactory) {
      this.serDes = serDes;
      this.typeFactory = typeFactory;
    }

    private String[] splitClassNames(String name) {
      return name.split("#");
    }

    private String getClassNameForCollection(String name, Collection<?> payload) {
      if (payload instanceof List) {
        if (payload.isEmpty()) {
          return null;
        }
        Object firstItem = ((List<?>) payload).get(0);
        // Only support non-generic item classes in lists to avoid ambiguous encoding
        if (firstItem.getClass().getTypeParameters().length > 0) {
          return null;
        }
        String itemClassName = getClassName(firstItem);
        if (itemClassName == null) {
          return null;
        }
        return name + '#' + itemClassName;
      }
      return null;
    }

    private Class<?> resolveTypeVariable(Class<?> clazz, TypeVariable<?> tv, Object payload) {
      // TypeVariable instances are scoped to the class that declares them, so
      // field.getGenericType().equals(tv) can only match fields declared on clazz itself.
      // Superclass fields reference their own TypeVariable instances, which are distinct objects.
      for (Field field : clazz.getDeclaredFields()) {
        if (field.getGenericType().equals(tv)) {
          field.setAccessible(true);
          try {
            Object value = field.get(payload);
            if (value != null) {
              return value.getClass();
            }
          } catch (IllegalAccessException e) {
            log.debug("Cannot access field {}", field.getName(), e);
          }
        }
      }
      return null;
    }

    private String getGenericFieldBasedClassName(Class<?> clazz, Object payload) {
      TypeVariable<?>[] typeVariables = clazz.getTypeParameters();
      if (typeVariables.length == 0) {
        return clazz.getName();
      }
      StringBuilder sb = new StringBuilder(clazz.getName());
      for (TypeVariable<?> tv : typeVariables) {
        Class<?> resolved = resolveTypeVariable(clazz, tv, payload);
        if (resolved == null || resolved.getTypeParameters().length > 0) {
          return null;
        }
        sb.append('#').append(resolved.getName());
      }
      return sb.toString();
    }

    private String getClassName(Object payload) {
      Class<?> payloadClass = payload.getClass();
      String name = payloadClass.getName();
      if (payload instanceof Collection) {
        return getClassNameForCollection(name, (Collection<?>) payload);
      }
      return getGenericFieldBasedClassName(payloadClass, payload);
    }

    private TypeEnvelop getTargetType(Msg msg) throws ClassNotFoundException {
      String[] classNames = splitClassNames(msg.getName());
      if (classNames.length == 1) {
        Class<?> c = Thread.currentThread().getContextClassLoader().loadClass(msg.getName());
        return typeFactory.create(c);
      }
      Class<?> envelopeClass =
          Thread.currentThread().getContextClassLoader().loadClass(classNames[0]);
      Class<?>[] classes = new Class<?>[classNames.length - 1];
      for (int i = 1; i < classNames.length; i++) {
        classes[i - 1] = Thread.currentThread().getContextClassLoader().loadClass(classNames[i]);
      }
      return typeFactory.create(envelopeClass, classes);
    }

    public Object deserialize(String payload) {
      try {
        if (SerializationUtils.isJson(payload)) {
          Msg msg = serDes.deserialize(payload, Msg.class);
          TypeEnvelop type = getTargetType(msg);
          return serDes.deserialize(msg.msg, type);
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
        return serDes.deserialize(payload, clazz);
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
        byte[] msg = serDes.serialize(payload);
        Msg message = new Msg(msg, name);
        return serDes.serializeAsString(message);
      } catch (Exception e) {
        log.debug("Serialisation failed", e);
        return null;
      }
    }
  }
}
