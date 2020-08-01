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

package com.github.sonus21.rqueue.core.support;

import com.github.sonus21.rqueue.converter.GenericMessageConverter;
import com.github.sonus21.rqueue.core.RqueueMessage;
import java.util.ArrayList;
import java.util.List;
import org.springframework.messaging.Message;

public class RqueueMessageFactory {
  private static final GenericMessageConverter converter = new GenericMessageConverter();

  RqueueMessageFactory() {}

  public static RqueueMessage buildMessage(
      Object object, String queueName, Integer retryCount, Long delay) {
    Object payload = object;
    if (object == null) {
      payload = "Test message";
    }
    Message<?> msg = converter.toMessage(payload, null);
    assert msg != null;
    return new RqueueMessage(queueName, (String) msg.getPayload(), retryCount, delay);
  }

  public static List<RqueueMessage> generateMessages(String queueName, int count) {
    return generateMessages(null, queueName, null, null, count);
  }

  public static List<RqueueMessage> generateMessages(String queueName, long delay, int count) {
    return generateMessages(null, queueName, null, delay, count);
  }

  public static List<RqueueMessage> generateMessages(
      Object object, String queueName, Integer retryCount, Long delay, int count) {
    List<RqueueMessage> messages = new ArrayList<>();
    for (int i = 0; i < count; i++) {
      messages.add(buildMessage(object, queueName, retryCount, delay));
    }
    return messages;
  }
}
