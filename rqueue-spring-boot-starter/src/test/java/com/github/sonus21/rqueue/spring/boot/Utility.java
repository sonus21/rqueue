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

package com.github.sonus21.rqueue.spring.boot;

import com.github.sonus21.rqueue.converter.GenericMessageConverter;
import com.github.sonus21.rqueue.core.RqueueMessage;
import org.springframework.messaging.Message;

public abstract class Utility {
  private static final GenericMessageConverter converter = new GenericMessageConverter();

  public static RqueueMessage buildMessage(
      Object object, String queueName, Integer retryCount, Long delay) {
    Message<?> msg = converter.toMessage(object, null);
    return new RqueueMessage(queueName, (String) msg.getPayload(), retryCount, delay);
  }
}
