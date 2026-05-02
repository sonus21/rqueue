/*
 * Copyright (c) 2021-2026 Sonu Kumar
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

package com.github.sonus21.rqueue.core;

import com.github.sonus21.rqueue.common.RqueueRedisTemplate;
import com.github.sonus21.rqueue.config.RqueueConfig;
import com.github.sonus21.rqueue.converter.RqueueRedisSerializer;
import com.github.sonus21.rqueue.listener.RqueueMessageListenerContainer;
import com.github.sonus21.rqueue.models.enums.PubSubType;
import com.github.sonus21.rqueue.models.event.RqueuePubSubEvent;
import com.github.sonus21.rqueue.models.request.PauseUnpauseQueueRequest;
import com.github.sonus21.rqueue.serdes.RqueueSerDes;
import com.github.sonus21.rqueue.serdes.SerializationUtils;
import com.github.sonus21.rqueue.utils.Constants;
import com.github.sonus21.rqueue.utils.StringUtils;
import java.time.Duration;
import java.util.UUID;
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
  private final RqueueRedisSerializer rqueueRedisSerializer;
  private final RqueueBeanProvider rqueueBeanProvider;
  private final RqueueSerDes serDes = SerializationUtils.getSerDes();

  public RqueueInternalPubSubChannel(
      RqueueRedisListenerContainerFactory rqueueRedisListenerContainerFactory,
      RqueueMessageListenerContainer rqueueMessageListenerContainer,
      RqueueConfig rqueueConfig,
      RqueueRedisTemplate<String> stringRqueueRedisTemplate,
      RqueueBeanProvider rqueueBeanProvider) {
    this.rqueueRedisListenerContainerFactory = rqueueRedisListenerContainerFactory;
    this.rqueueMessageListenerContainer = rqueueMessageListenerContainer;
    this.rqueueConfig = rqueueConfig;
    this.stringRqueueRedisTemplate = stringRqueueRedisTemplate;
    this.rqueueBeanProvider = rqueueBeanProvider;
    this.rqueueRedisSerializer = new RqueueRedisSerializer();
  }

  @Override
  public void afterPropertiesSet() throws Exception {
    String channel = rqueueConfig.getInternalCommChannelName();
    rqueueRedisListenerContainerFactory.addMessageListener(
        new InternalMessageListener(), new ChannelTopic(channel));
  }

  public void emitPauseUnpauseQueueEvent(PauseUnpauseQueueRequest pauseUnpauseQueueRequest) {
    publish(PubSubType.PAUSE_QUEUE, pauseUnpauseQueueRequest);
  }

  private void publish(PubSubType type, Object message) {
    byte[] data = rqueueRedisSerializer.serialize(message);
    RqueuePubSubEvent event =
        new RqueuePubSubEvent(type, RqueueConfig.getBrokerId(), new String(data));
    stringRqueueRedisTemplate
        .getRedisTemplate()
        .convertAndSend(rqueueConfig.getInternalCommChannelName(), event);
  }

  public void emitQueueConfigUpdateEvent(PauseUnpauseQueueRequest request) {
    publish(PubSubType.QUEUE_CRUD, request.getName());
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
      log.debug("Message on internal channel {}", new String(body));
      RqueuePubSubEvent rqueuePubSubEvent;
      try {
        rqueuePubSubEvent = serDes.deserialize(body, RqueuePubSubEvent.class);
      } catch (Exception e) {
        log.error("Invalid message on pub-sub channel {}", new String(body), e);
        return;
      }
      if (rqueuePubSubEvent == null) {
        log.error("Invalid message on pub-sub channel {}", new String(body));
        return;
      }
      try {
        switch (rqueuePubSubEvent.getType()) {
          case PAUSE_QUEUE:
            handlePauseEvent(rqueuePubSubEvent.messageAs(serDes, PauseUnpauseQueueRequest.class));
            break;
          case QUEUE_CRUD:
            rqueueBeanProvider
                .getRqueueSystemConfigDao()
                .clearCacheByName(rqueuePubSubEvent.messageAs(serDes, String.class));
            break;
          default:
            log.error("Unknown event type {}", rqueuePubSubEvent);
        }
      } catch (Exception e) {
        log.error("Failed to process pub-sub event {}", rqueuePubSubEvent, e);
      }
    }

    private void handlePauseEvent(PauseUnpauseQueueRequest request) {
      if (request == null || StringUtils.isEmpty(request.getName())) {
        log.error("Invalid message payload {}", request);
        return;
      }
      String lockKey = Constants.getQueueCrudLockKey(rqueueConfig, request.getName());
      String lockValue = UUID.randomUUID().toString();
      try {
        boolean acquired = rqueueBeanProvider
            .getRqueueLockManager()
            .acquireLock(lockKey, lockValue, Duration.ofMillis(100));
        if (acquired) {
          rqueueMessageListenerContainer.pauseUnpauseQueue(request.getName(), request.isPause());
        }
      } finally {
        rqueueBeanProvider.getRqueueLockManager().releaseLock(lockKey, lockValue);
      }
    }
  }
}
