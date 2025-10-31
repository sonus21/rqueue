/*
 * Copyright (c) 2023 Sonu Kumar
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

package com.github.sonus21.rqueue.utils;

import com.github.sonus21.rqueue.core.DefaultRqueueMessageConverter;
import com.github.sonus21.rqueue.core.RqueueMessage;
import com.github.sonus21.rqueue.core.support.RqueueMessageUtils;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import org.springframework.messaging.converter.MessageConverter;

@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class RqueueMessageTestUtils {

  public static RqueueMessage createMessage(String queueName) {
    return createMessage(new DefaultRqueueMessageConverter(), queueName);
  }


  public static RqueueMessage createMessage(MessageConverter messageConverter, String queue) {
    return RqueueMessageUtils.generateMessage(messageConverter, queue);
  }
}
