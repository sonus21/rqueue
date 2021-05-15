/*
 *  Copyright 2021 Sonu Kumar
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *         https://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and limitations under the License.
 *
 */

package com.github.sonus21.rqueue.core;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.sonus21.rqueue.common.RqueueRedisTemplate;
import com.github.sonus21.rqueue.config.RqueueConfig;
import com.github.sonus21.rqueue.converter.GenericMessageConverter.SmartMessageSerDes;
import com.github.sonus21.rqueue.listener.RqueueMessageListenerContainer;
import com.github.sonus21.rqueue.models.enums.PubSubType;
import com.github.sonus21.rqueue.models.event.RqueuePubSubEvent;
import com.github.sonus21.rqueue.utils.SerializationUtils;
import com.github.sonus21.rqueue.utils.StringUtils;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.data.redis.connection.Message;
import org.springframework.data.redis.connection.MessageListener;
import org.springframework.data.redis.listener.ChannelTopic;

@Slf4j
public class RqueueInternalPubSubChannel implements InitializingBean {
  private final RqueueRedisListenerContainerFactory rqueueRedisListenerContainerFactory;
  private final RqueueMessageListenerContainer rqueueMessageListenerContainer;
  private final RqueueConfig rqueueConfig;
  private final RqueueRedisTemplate<String> stringRqueueRedisTemplate;

  private SmartMessageSerDes smartMessageSerDes;

  public RqueueInternalPubSubChannel(
      RqueueRedisListenerContainerFactory rqueueRedisListenerContainerFactory,
      RqueueMessageListenerContainer rqueueMessageListenerContainer,
      RqueueConfig rqueueConfig,
      RqueueRedisTemplate<String> stringRqueueRedisTemplate) {
    this.rqueueRedisListenerContainerFactory = rqueueRedisListenerContainerFactory;
    this.rqueueMessageListenerContainer = rqueueMessageListenerContainer;
    this.rqueueConfig = rqueueConfig;
    this.stringRqueueRedisTemplate = stringRqueueRedisTemplate;
  }

  @Override
  public void afterPropertiesSet() throws Exception {
    String channel = rqueueConfig.getInternalCommChannelName();
    InternalMessageListener messageListener = new InternalMessageListener();
    rqueueRedisListenerContainerFactory.addMessageListener(
        messageListener, new ChannelTopic(channel));
    ObjectMapper mapper = new ObjectMapper();
    mapper = mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    this.smartMessageSerDes = new SmartMessageSerDes(mapper);
  }

  public void emitPauseUnpauseQueueEvent(String queueName) {
    publish(new RqueuePubSubEvent(PubSubType.PAUSE_QUEUE, RqueueConfig.getBrokerId(), queueName));
  }

  private void publish(RqueuePubSubEvent rqueuePubSubEvent) {
    stringRqueueRedisTemplate
        .getRedisTemplate()
        .convertAndSend(rqueueConfig.getInternalCommChannelName(), rqueuePubSubEvent);
  }

  class InternalMessageListener implements MessageListener {
    @Override
    public void onMessage(Message message, byte[] pattern) {
      byte[] body = message.getBody();
      if (SerializationUtils.isEmpty(body)) {
        log.error(
            "Empty message received on channel: {}, pattern: {}",
            new String(message.getChannel()),
            new String(pattern));
        return;
      }
      processEvent(body);
    }

    private void processEvent(byte[] body) {
      RqueuePubSubEvent rqueuePubSubEvent =
          smartMessageSerDes.deserialize(body, RqueuePubSubEvent.class);
      if (rqueuePubSubEvent == null) {
        log.error("Invalid message on pub-sub channel {}", new String(body));
        return;
      }
      switch (rqueuePubSubEvent.getType()) {
        case PAUSE_QUEUE:
          handlePauseEvent(rqueuePubSubEvent);
          break;
        default:
          log.error("Unknown event type {}", rqueuePubSubEvent);
      }
    }

    private void handlePauseEvent(RqueuePubSubEvent rqueuePubSubEvent) {
      if (StringUtils.isEmpty(rqueuePubSubEvent.getMessage())) {
        log.error("Invalid message payload {}", rqueuePubSubEvent);
        return;
      }
      rqueueMessageListenerContainer.pauseUnpauseQueue(rqueuePubSubEvent.getMessage());
    }
  }
}
