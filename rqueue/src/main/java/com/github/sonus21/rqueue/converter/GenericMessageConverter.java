package com.github.sonus21.rqueue.converter;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.sonus21.rqueue.exception.MessageDeserializationError;
import com.github.sonus21.rqueue.exception.MessageSerializationError;
import java.io.IOException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageHeaders;
import org.springframework.messaging.converter.MessageConverter;
import org.springframework.messaging.support.GenericMessage;

public class GenericMessageConverter implements MessageConverter {
  private static ObjectMapper objectMapper = new ObjectMapper();
  private static Logger logger = LoggerFactory.getLogger(GenericMessageConverter.class);

  @Override
  public Object fromMessage(Message<?> message, Class<?> targetClass) {
    try {
      Msg msg = objectMapper.readValue((String) message.getPayload(), Msg.class);
      Class<?> c = Class.forName(msg.getName());
      return objectMapper.readValue(msg.msg, c);
    } catch (ClassNotFoundException e) {
      logger.error("Class not found exception", e);
      throw new MessageDeserializationError(e.getMessage(), e);
    } catch (JsonParseException e) {
      throw new MessageDeserializationError(e.getMessage(), e);
    } catch (IOException e) {
      throw new MessageDeserializationError(e.getMessage(), e);
    }
  }

  @Override
  public Message<?> toMessage(Object object, MessageHeaders headers) {
    String name = object.getClass().getName();
    try {
      String msg = objectMapper.writeValueAsString(object);
      Msg message = new Msg(msg, name);
      return new GenericMessage<>(objectMapper.writeValueAsString(message));
    } catch (JsonProcessingException e) {
      logger.error("Serialisation failed", e);
      throw new MessageSerializationError(e.getMessage(), e);
    }
  }

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
