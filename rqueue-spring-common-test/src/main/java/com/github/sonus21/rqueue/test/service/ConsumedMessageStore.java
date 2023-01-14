/*
 * Copyright (c) 2019-2023 Sonu Kumar
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

package com.github.sonus21.rqueue.test.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.sonus21.rqueue.test.dto.BaseQueueMessage;
import com.github.sonus21.rqueue.test.entity.ConsumedMessage;
import com.github.sonus21.rqueue.test.repository.ConsumedMessageRepository;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service
@RequiredArgsConstructor(onConstructor = @__(@Autowired))
@Slf4j
public class ConsumedMessageStore {

  @NonNull
  private final ConsumedMessageRepository consumedMessageRepository;
  @NonNull
  private final ObjectMapper objectMapper;

  public void save(BaseQueueMessage message, Object tag, String queueName)
      throws JsonProcessingException {
    log.info("Queue '{}' Message: {} Tag: '{}'", queueName, message, tag);
    String tagStr;
    if (tag == null) {
      tagStr = null;
    } else if (tag instanceof String) {
      tagStr = (String) tag;
    } else {
      tagStr = objectMapper.writeValueAsString(tag);
    }
    ConsumedMessage consumedMessage =
        consumedMessageRepository.findByMessageIdAndTag(message.getId(), tagStr);
    String textMessage = objectMapper.writeValueAsString(message);
    if (consumedMessage == null) {
      consumedMessage = new ConsumedMessage(message.getId(), tagStr, queueName, textMessage);
    } else {
      consumedMessage.incrementCount();
      consumedMessage.setMessage(textMessage);
    }
    consumedMessageRepository.save(consumedMessage);
    log.info("Message saved {}", consumedMessage);
  }

  public Collection<ConsumedMessage> getConsumedMessages(Collection<String> messageIds) {
    return getMessages(messageIds).values();
  }

  public int getConsumedMessageCount(String messageId) {
    return getConsumedMessages(messageId).size();
  }

  public <T> T getMessage(String messageId, Class<T> tClass) {
    return getMessages(Collections.singletonList(messageId), tClass).get(messageId);
  }

  public <T> Map<String, T> getMessages(Collection<String> messageIds, Class<T> tClass) {
    Map<String, T> idToMessage = new HashMap<>();
    getMessages(messageIds)
        .values()
        .forEach(
            consumedMessage -> {
              try {
                T value = objectMapper.readValue(consumedMessage.getMessage(), tClass);
                idToMessage.put(consumedMessage.getMessageId(), value);
              } catch (JsonProcessingException e) {
                e.printStackTrace();
              }
            });
    return idToMessage;
  }

  public Map<String, ConsumedMessage> getMessages(Collection<String> messageIds) {
    Iterable<ConsumedMessage> consumedMessages =
        consumedMessageRepository.findByMessageIdIn(messageIds);
    Map<String, ConsumedMessage> idToMessage = new HashMap<>();
    consumedMessages.forEach(
        consumedMessage -> idToMessage.put(consumedMessage.getMessageId(), consumedMessage));
    return idToMessage;
  }

  public List<ConsumedMessage> getAllMessages() {
    List<ConsumedMessage> consumedMessages = new ArrayList<>();
    for (ConsumedMessage consumedMessage : consumedMessageRepository.findAll()) {
      consumedMessages.add(consumedMessage);
    }
    return consumedMessages;
  }

  public ConsumedMessage getConsumedMessage(String messageId) {
    List<ConsumedMessage> messages = getConsumedMessages(messageId);
    if (messages.isEmpty()) {
      return null;
    }
    if (messages.size() == 1) {
      return messages.get(0);
    }
    throw new IllegalStateException("more than one record found");
  }

  public List<ConsumedMessage> getConsumedMessages(String messageId) {
    return consumedMessageRepository.findByMessageId(messageId);
  }

  public List<ConsumedMessage> getConsumedMessagesForQueue(String queueName) {
    return consumedMessageRepository.findByQueueName(queueName);
  }
}
