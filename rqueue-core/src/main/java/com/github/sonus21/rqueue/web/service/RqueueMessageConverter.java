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

package com.github.sonus21.rqueue.web.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.github.sonus21.rqueue.models.PubSubMessage;
import java.io.IOException;
import org.springframework.data.redis.connection.Message;

public interface RqueueMessageConverter {
  byte[] toMessage(PubSubMessage message) throws JsonProcessingException;

  PubSubMessage fromMessage(Message message) throws IOException;

  String fromMessage(com.github.sonus21.rqueue.models.request.Message message)
      throws JsonProcessingException;

  com.github.sonus21.rqueue.models.request.Message toMessage(String message) throws IOException;
}
