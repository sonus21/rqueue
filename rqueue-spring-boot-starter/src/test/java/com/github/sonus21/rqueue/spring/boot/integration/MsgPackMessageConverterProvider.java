/*
 * Copyright (c) 2026 Sonu Kumar
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
package com.github.sonus21.rqueue.spring.boot.integration;

import com.github.sonus21.rqueue.converter.MessageConverterProvider;
import java.io.IOException;
import java.util.Base64;
import org.msgpack.core.MessageBufferPacker;
import org.msgpack.core.MessagePack;
import org.msgpack.core.MessageUnpacker;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageHeaders;
import org.springframework.messaging.converter.MessageConversionException;
import org.springframework.messaging.converter.MessageConverter;
import org.springframework.messaging.support.GenericMessage;

public class MsgPackMessageConverterProvider implements MessageConverterProvider {

  private static final String PREFIX = "msgpack:";

  @Override
  public MessageConverter getConverter() {
    return new MsgPackMessageConverter();
  }

  static boolean isMsgPack(String payload) {
    return payload != null && payload.startsWith(PREFIX);
  }

  static MessagePackageListenerTest.ListenerPayload decode(String payload) {
    if (!isMsgPack(payload)) {
      throw new MessageConversionException("Payload is not MsgPack encoded");
    }
    return MsgPackCodec.decode(Base64.getDecoder().decode(payload.substring(PREFIX.length())));
  }

  private static class MsgPackMessageConverter implements MessageConverter {

    @Override
    public Object fromMessage(Message<?> message, Class<?> targetClass) {
      Object payload = message.getPayload();
      if (payload instanceof MessagePackageListenerTest.ListenerPayload) {
        return payload;
      }
      if (!(payload instanceof String)) {
        return null;
      }
      return decode((String) payload);
    }

    @Override
    public Message<?> toMessage(Object payload, MessageHeaders headers) {
      if (payload instanceof MessagePackageListenerTest.ListenerPayload) {
        byte[] msgPackBytes =
            MsgPackCodec.encode((MessagePackageListenerTest.ListenerPayload) payload);
        return new GenericMessage<>(PREFIX + Base64.getEncoder().encodeToString(msgPackBytes));
      }
      if (payload instanceof String) {
        return new GenericMessage<>(payload);
      }
      return null;
    }
  }

  private static final class MsgPackCodec {

    private MsgPackCodec() {}

    static byte[] encode(MessagePackageListenerTest.ListenerPayload payload) {
      try (MessageBufferPacker packer = MessagePack.newDefaultBufferPacker()) {
        packer.packMapHeader(2);
        packer.packString("backend");
        packer.packString(payload.getBackend());
        packer.packString("body");
        packer.packString(payload.getBody());
        return packer.toByteArray();
      } catch (IOException e) {
        throw new MessageConversionException("MsgPack encoding failed", e);
      }
    }

    static MessagePackageListenerTest.ListenerPayload decode(byte[] bytes) {
      try (MessageUnpacker unpacker = MessagePack.newDefaultUnpacker(bytes)) {
        int entries = unpacker.unpackMapHeader();
        String backend = null;
        String body = null;
        for (int i = 0; i < entries; i++) {
          String key = unpacker.unpackString();
          String value = unpacker.unpackString();
          if ("backend".equals(key)) {
            backend = value;
          } else if ("body".equals(key)) {
            body = value;
          }
        }
        return new MessagePackageListenerTest.ListenerPayload(backend, body);
      } catch (IOException e) {
        throw new MessageConversionException("MsgPack decoding failed", e);
      }
    }
  }
}
