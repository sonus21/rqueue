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

import com.github.sonus21.rqueue.exception.ProcessingException;
import com.github.sonus21.rqueue.models.PubSubMessage;
import com.github.sonus21.rqueue.models.enums.EventChannelType;
import com.github.sonus21.rqueue.models.enums.EventType;
import com.github.sonus21.rqueue.models.enums.PubSubMessageSenderRole;

public interface RqueueRedisMessagePublisher {
  void publish(String topic, PubSubMessage message) throws ProcessingException;

  default void publish(
      PubSubMessageSenderRole role, EventChannelType channelType, EventType eventType)
      throws ProcessingException {
    publish(role, channelType, eventType, null);
  }

  default void publishBrokerQueue(EventType eventType, Object message) throws ProcessingException {
    publish(PubSubMessageSenderRole.BROKER, EventChannelType.QUEUE, eventType, message);
  }

  default void publishListenerQueue(EventType eventType) throws ProcessingException {
    publish(PubSubMessageSenderRole.LISTENER, EventChannelType.QUEUE, eventType, null);
  }

  default void publishBrokerTopic(EventType eventType, Object message) throws ProcessingException {
    publish(PubSubMessageSenderRole.BROKER, EventChannelType.TOPIC, eventType, message);
  }

  void publish(
      PubSubMessageSenderRole role,
      EventChannelType channelType,
      EventType eventType,
      Object message)
      throws ProcessingException;
}
