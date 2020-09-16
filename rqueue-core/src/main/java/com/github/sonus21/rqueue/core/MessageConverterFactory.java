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

package com.github.sonus21.rqueue.core;

import com.github.sonus21.rqueue.converter.GenericMessageConverter;
import java.util.ArrayList;
import java.util.List;
import org.springframework.messaging.converter.CompositeMessageConverter;
import org.springframework.messaging.converter.MessageConverter;
import org.springframework.messaging.converter.StringMessageConverter;

public final class MessageConverterFactory {
  private MessageConverterFactory() {}

  public static CompositeMessageConverter getMessageConverter(
      List<MessageConverter> messageConverterList) {
    List<MessageConverter> messageConverters = messageConverterList;
    if (messageConverterList == null) {
      messageConverters = new ArrayList<>();
    }
    messageConverters = new ArrayList<>(messageConverters);
    // String message converter
    StringMessageConverter stringMessageConverter = new StringMessageConverter();
    stringMessageConverter.setSerializedPayloadClass(String.class);
    messageConverters.add(stringMessageConverter);
    // add generic message converter
    messageConverters.add(new GenericMessageConverter());
    return new CompositeMessageConverter(messageConverters);
  }
}
