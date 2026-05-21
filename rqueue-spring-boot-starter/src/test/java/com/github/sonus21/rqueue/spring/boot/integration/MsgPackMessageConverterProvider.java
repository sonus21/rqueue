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
import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
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
      ByteArrayOutputStream out = new ByteArrayOutputStream();
      out.write(0x82);
      writeString(out, "backend");
      writeString(out, payload.getBackend());
      writeString(out, "body");
      writeString(out, payload.getBody());
      return out.toByteArray();
    }

    static MessagePackageListenerTest.ListenerPayload decode(byte[] bytes) {
      Cursor cursor = new Cursor(bytes);
      int mapHeader = cursor.readUnsignedByte();
      int entries;
      if ((mapHeader & 0xf0) == 0x80) {
        entries = mapHeader & 0x0f;
      } else {
        throw new MessageConversionException("Expected MsgPack fixmap");
      }
      String backend = null;
      String body = null;
      for (int i = 0; i < entries; i++) {
        String key = readString(cursor);
        String value = readString(cursor);
        if ("backend".equals(key)) {
          backend = value;
        } else if ("body".equals(key)) {
          body = value;
        }
      }
      return new MessagePackageListenerTest.ListenerPayload(backend, body);
    }

    private static void writeString(ByteArrayOutputStream out, String value) {
      byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
      if (bytes.length <= 31) {
        out.write(0xa0 | bytes.length);
      } else if (bytes.length <= 255) {
        out.write(0xd9);
        out.write(bytes.length);
      } else {
        throw new MessageConversionException("Test MsgPack codec supports strings up to 255 bytes");
      }
      out.writeBytes(bytes);
    }

    private static String readString(Cursor cursor) {
      int header = cursor.readUnsignedByte();
      int length;
      if ((header & 0xe0) == 0xa0) {
        length = header & 0x1f;
      } else if (header == 0xd9) {
        length = cursor.readUnsignedByte();
      } else {
        throw new MessageConversionException("Expected MsgPack string");
      }
      return new String(cursor.readBytes(length), StandardCharsets.UTF_8);
    }
  }

  private static final class Cursor {

    private final byte[] bytes;
    private int index;

    Cursor(byte[] bytes) {
      this.bytes = bytes;
    }

    int readUnsignedByte() {
      if (index >= bytes.length) {
        throw new MessageConversionException("Unexpected end of MsgPack payload");
      }
      return bytes[index++] & 0xff;
    }

    byte[] readBytes(int length) {
      if (index + length > bytes.length) {
        throw new MessageConversionException("Unexpected end of MsgPack payload");
      }
      byte[] value = new byte[length];
      System.arraycopy(bytes, index, value, 0, length);
      index += length;
      return value;
    }
  }
}
