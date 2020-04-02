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

package com.github.sonus21.rqueue.utils;

import java.util.List;
import org.springframework.messaging.Message;
import org.springframework.messaging.converter.MessageConverter;
import org.springframework.util.Assert;

public abstract class MessageUtility {
  public static Object convertMessageToObject(
      Message<?> message, List<MessageConverter> messageConverters) {
    Assert.notEmpty(messageConverters, "messageConverters cannot be empty");
    for (MessageConverter messageConverter : messageConverters) {
      try {
        return messageConverter.fromMessage(message, null);
      } catch (Exception e) {
      }
    }
    return null;
  }
}
