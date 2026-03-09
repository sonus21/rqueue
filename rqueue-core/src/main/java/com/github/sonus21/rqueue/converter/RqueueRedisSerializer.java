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

package com.github.sonus21.rqueue.converter;

import com.fasterxml.jackson.annotation.JsonTypeInfo.As;
import com.github.sonus21.rqueue.utils.SerializationUtils;
import lombok.extern.slf4j.Slf4j;
import org.springframework.cache.support.NullValue;
import org.springframework.data.redis.serializer.RedisSerializer;
import org.springframework.data.redis.serializer.SerializationException;
import tools.jackson.core.JacksonException;
import tools.jackson.core.JsonGenerator;
import tools.jackson.databind.DefaultTyping;
import tools.jackson.databind.ObjectMapper;
import tools.jackson.databind.SerializationContext;
import tools.jackson.databind.jsontype.BasicPolymorphicTypeValidator;
import tools.jackson.databind.module.SimpleModule;
import tools.jackson.databind.ser.std.StdSerializer;

@Slf4j
public class RqueueRedisSerializer implements RedisSerializer<Object> {

  private final RedisSerializer<Object> serializer;

  public RqueueRedisSerializer(RedisSerializer<Object> redisSerializer) {
    this.serializer = redisSerializer;
  }

  public RqueueRedisSerializer() {
    this(new RqueueRedisSerDes());
  }

  @Override
  public byte[] serialize(Object t) throws SerializationException {
    return serializer.serialize(t);
  }

  @Override
  public Object deserialize(byte[] bytes) throws SerializationException {
    if (SerializationUtils.isEmpty(bytes)) {
      return null;
    }
    try {
      return serializer.deserialize(bytes);
    } catch (Exception e) {
      log.warn("Deserialization has failed {}", new String(bytes), e);
      return new String(bytes);
    }
  }

  // adapted from spring-data-redis
  private static class RqueueRedisSerDes implements RedisSerializer<Object> {

    private ObjectMapper mapper;

    RqueueRedisSerDes() {
      this.mapper = SerializationUtils.createObjectMapper().rebuild()
          .addModule(new SimpleModule().addSerializer(new NullValueSerializer()))
          .activateDefaultTyping(BasicPolymorphicTypeValidator.builder()
              .allowIfSubType(Object.class)
              .build(), DefaultTyping.NON_FINAL, As.PROPERTY)
          .build();
    }

    @Override
    public byte[] serialize(Object source) throws SerializationException {
      if (source == null) {
        return SerializationUtils.EMPTY_ARRAY;
      }
      try {
        return mapper.writeValueAsBytes(source);
      } catch (JacksonException e) {
        throw new SerializationException("Could not write JSON: " + e.getMessage(), e);
      }
    }

    @Override
    public Object deserialize(byte[] source) throws SerializationException {
      if (SerializationUtils.isEmpty(source)) {
        return null;
      }
      try {
        return mapper.readValue(source, Object.class);
      } catch (Exception ex) {
        throw new SerializationException("Could not read JSON: " + ex.getMessage(), ex);
      }
    }

    private static class NullValueSerializer extends StdSerializer<NullValue> {

      private static final long serialVersionUID = 211020517180777825L;
      private final String classIdentifier;

      NullValueSerializer() {
        super(NullValue.class);
        this.classIdentifier = "@class";
      }

      @Override
      public void serialize(NullValue value, JsonGenerator jsonGenerator, SerializationContext provider) throws JacksonException {
        jsonGenerator.writeStartObject();
        jsonGenerator.writeStringProperty(classIdentifier, NullValue.class.getName());
        jsonGenerator.writeEndObject();
      }
    }
  }
}
