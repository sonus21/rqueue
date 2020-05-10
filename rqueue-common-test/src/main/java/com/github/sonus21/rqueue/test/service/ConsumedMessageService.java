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

package com.github.sonus21.rqueue.test.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.sonus21.rqueue.test.dto.BaseQueueMessage;
import com.github.sonus21.rqueue.test.entity.ConsumedMessage;
import com.github.sonus21.rqueue.test.repository.ConsumedMessageRepository;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service
@RequiredArgsConstructor(onConstructor = @__(@Autowired))
public class ConsumedMessageService {
  @NonNull private ConsumedMessageRepository consumedMessageRepository;
  @NonNull private ObjectMapper objectMapper;

  public <T extends BaseQueueMessage> ConsumedMessage save(BaseQueueMessage message)
      throws JsonProcessingException {
    String textMessage = objectMapper.writeValueAsString(message);
    ConsumedMessage consumedMessage = new ConsumedMessage(message.getId(), textMessage);
    consumedMessageRepository.save(consumedMessage);
    return consumedMessage;
  }

  public <T> T getMessage(String id, Class<T> tClass) {
    return getMessages(Collections.singletonList(id), tClass).get(id);
  }

  public <T> Map<String, T> getMessages(Collection<String> ids, Class<T> tClass) {
    Iterable<ConsumedMessage> consumedMessages = consumedMessageRepository.findAllById(ids);
    Map<String, T> idToMessage = new HashMap<>();
    consumedMessages.forEach(
        consumedMessage -> {
          try {
            T value = objectMapper.readValue(consumedMessage.getMessage(), tClass);
            idToMessage.put(consumedMessage.getId(), value);
          } catch (JsonProcessingException e) {
            e.printStackTrace();
          }
        });
    return idToMessage;
  }
}
