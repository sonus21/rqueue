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

package com.github.sonus21.rqueue.web.service.impl;

import com.github.sonus21.rqueue.common.RqueueRedisTemplate;
import com.github.sonus21.rqueue.config.RqueueConfig;
import com.github.sonus21.rqueue.exception.ProcessingException;
import com.github.sonus21.rqueue.models.PubSubMessage;
import com.github.sonus21.rqueue.models.enums.EventChannelType;
import com.github.sonus21.rqueue.models.enums.EventType;
import com.github.sonus21.rqueue.models.enums.PubSubMessageSenderRole;
import com.github.sonus21.rqueue.web.service.RqueueMessageConverter;
import com.github.sonus21.rqueue.web.service.RqueueRedisMessagePublisher;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class RqueueRedisMessagePublisherImpl implements RqueueRedisMessagePublisher {
  private final RqueueRedisTemplate<String> stringRqueueRedisTemplate;
  private final RqueueMessageConverter rqueueMessageConverter;
  private final RqueueConfig rqueueConfig;

  @Autowired
  public RqueueRedisMessagePublisherImpl(
      RqueueRedisTemplate<String> stringRqueueRedisTemplate,
      RqueueMessageConverter rqueueMessageConverter,
      RqueueConfig rqueueConfig) {
    this.stringRqueueRedisTemplate = stringRqueueRedisTemplate;
    this.rqueueMessageConverter = rqueueMessageConverter;
    this.rqueueConfig = rqueueConfig;
  }

  @Override
  public void publish(String topic, PubSubMessage message) throws ProcessingException {
    this.stringRqueueRedisTemplate
        .getRedisTemplate()
        .convertAndSend(topic, rqueueMessageConverter.toMessage(message));
  }

  @Override
  public void publish(
      PubSubMessageSenderRole role,
      EventChannelType channelType,
      EventType eventType,
      Object message)
      throws ProcessingException {
    PubSubMessage pubSubMessage =
        PubSubMessage.builder()
            .senderId(rqueueConfig.getBrokerId())
            .role(role)
            .eventChannel(channelType)
            .eventType(eventType)
            .message(message)
            .build();
    publish(rqueueConfig.getInternalEventChannel(), pubSubMessage);
  }
}
