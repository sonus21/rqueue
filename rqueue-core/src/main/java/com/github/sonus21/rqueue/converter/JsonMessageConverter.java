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

import static org.springframework.util.Assert.notNull;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.sonus21.rqueue.utils.SerializationUtils;
import lombok.extern.slf4j.Slf4j;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageHeaders;
import org.springframework.messaging.converter.MappingJackson2MessageConverter;
import org.springframework.messaging.converter.MessageConverter;
import org.springframework.messaging.support.GenericMessage;

/**
 * JsonMessageConverter tries to convert to JSON and from JSON to object.
 *
 * <p>Message converter relies on target class information to deserialize JSON to object. If it
 * finds target class is null then it returns the null.
 *
 * <p>Target class is null till the time method arguments are not resolved, once method arguments
 * are resolved then it will become non-null.
 *
 * @see MappingJackson2MessageConverter
 */
@Slf4j
public class JsonMessageConverter implements MessageConverter {

  private final ObjectMapper objectMapper;

  public JsonMessageConverter() {
    this.objectMapper = SerializationUtils.createObjectMapper();
  }

  public JsonMessageConverter(ObjectMapper objectMapper) {
    notNull(objectMapper, "objectMapper cannot be null");
    this.objectMapper = objectMapper;
  }

  @Override
  public Object fromMessage(Message<?> message, Class<?> targetClass) {
    log.trace("Message: {} TargetClass: {}", message.getPayload(), targetClass);
    try {
      String payload = (String) message.getPayload();
      if (targetClass == null) {
        return null;
      }
      if (SerializationUtils.isJson(payload)) {
        return objectMapper.readValue(payload, targetClass);
      }
      return null;
    } catch (JsonProcessingException | ClassCastException e) {
      log.debug("Deserialization of message {} failed", message, e);
      return null;
    }
  }

  @Override
  public Message<?> toMessage(Object payload, MessageHeaders headers) {
    log.trace("Payload: {} Headers: {}", payload, headers);
    try {
      String msg = objectMapper.writeValueAsString(payload);
      return new GenericMessage<>(msg);
    } catch (JsonProcessingException e) {
      log.debug("Serialisation failed, Payload: {}", payload, e);
      return null;
    }
  }
}
