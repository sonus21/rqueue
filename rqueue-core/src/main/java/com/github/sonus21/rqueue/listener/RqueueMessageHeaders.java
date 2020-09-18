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

package com.github.sonus21.rqueue.listener;

import com.github.sonus21.rqueue.core.RqueueMessage;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.springframework.messaging.MessageHeaders;

public final class RqueueMessageHeaders {
  public static final String DESTINATION = "destination";
  public static final String ID = "messageId";
  public static final String MESSAGE = "message";
  private static final MessageHeaders emptyMessageHeaders =
      new MessageHeaders(Collections.emptyMap());

  private RqueueMessageHeaders() {}

  public static MessageHeaders emptyMessageHeaders() {
    return emptyMessageHeaders;
  }

  static MessageHeaders buildMessageHeaders(String destination, RqueueMessage rqueueMessage) {
    Map<String, Object> headers = new HashMap<>(3);
    headers.put(DESTINATION, destination);
    headers.put(ID, rqueueMessage.getId());
    headers.put(MESSAGE, rqueueMessage);
    return new MessageHeaders(headers);
  }
}
