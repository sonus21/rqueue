/*
 * Copyright (c)  2019-2019, Sonu Kumar
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *       https://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */

package com.github.sonus21.rqueue.converter;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageHeaders;
import org.springframework.messaging.converter.MessageConverter;
import org.springframework.messaging.support.GenericMessage;

/**
 * A converter to turn the payload of a {@link Message} from serialized form to a typed String and
 * vice versa.
 */
public class GenericMessageConverter implements MessageConverter {
  private static ObjectMapper objectMapper = new ObjectMapper();
  private static Logger logger = LoggerFactory.getLogger(GenericMessageConverter.class);

  /**
   * Convert the payload of a {@link Message} from a serialized form to a typed Object of type
   * stored in message it self.
   *
   * <p>If the converter cannot perform the conversion it returns {@code null}.
   *
   * @param message the input message
   * @param targetClass the target class for the conversion
   * @return the result of the conversion, or {@code null} if the converter cannot perform the
   *     conversion
   */
  @Override
  public Object fromMessage(Message<?> message, Class<?> targetClass) {
    try {
      Msg msg = objectMapper.readValue((String) message.getPayload(), Msg.class);
      Class<?> c = Class.forName(msg.getName());
      return objectMapper.readValue(msg.msg, c);
    } catch (IOException | ClassCastException | ClassNotFoundException e) {
      logger.warn("Exception", e);
      return null;
    }
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
    String name = payload.getClass().getName();
    try {
      String msg = objectMapper.writeValueAsString(payload);
      Msg message = new Msg(msg, name);
      return new GenericMessage<>(objectMapper.writeValueAsString(message));
    } catch (JsonProcessingException e) {
      logger.error("Serialisation failed", e);
      return null;
    }
  }

  @SuppressWarnings({"UnusedDeclaration", "WeakerAccess"})
  private static class Msg {
    private String msg;
    private String name;

    public Msg() {}

    public Msg(String msg, String name) {
      this.msg = msg;
      this.name = name;
    }

    public String getMsg() {
      return msg;
    }

    public void setMsg(String msg) {
      this.msg = msg;
    }

    public String getName() {
      return name;
    }

    public void setName(String name) {
      this.name = name;
    }
  }
}
