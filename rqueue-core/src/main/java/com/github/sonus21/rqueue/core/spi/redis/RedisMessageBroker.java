/*
 * Copyright (c) 2020-2026 Sonu Kumar
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

package com.github.sonus21.rqueue.core.spi.redis;

import com.github.sonus21.rqueue.core.RqueueMessage;
import com.github.sonus21.rqueue.core.RqueueMessageTemplate;
import com.github.sonus21.rqueue.core.spi.Capabilities;
import com.github.sonus21.rqueue.core.spi.MessageBroker;
import com.github.sonus21.rqueue.listener.QueueDetail;
import com.github.sonus21.rqueue.models.MessageMoveResult;
import com.github.sonus21.rqueue.utils.RedisUtils;
import java.time.Duration;
import java.util.List;
import java.util.function.Consumer;
import org.springframework.data.redis.connection.Message;
import org.springframework.data.redis.connection.MessageListener;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.listener.ChannelTopic;
import org.springframework.data.redis.listener.RedisMessageListenerContainer;

/**
 * Default {@link MessageBroker} implementation that delegates to the existing Redis-backed
 * code path via {@link RqueueMessageTemplate}.
 *
 * <p>This is a thin Phase 1 wrapper: every method routes to the same call site that the existing
 * public API uses. No Lua scripts, DAO impls, or message flows are duplicated here. The intent is
 * to introduce the SPI seam without changing observable Redis behavior.
 */
public class RedisMessageBroker implements MessageBroker {

  private final RqueueMessageTemplate template;
  private final RedisMessageListenerContainer pubSubContainer;

  public RedisMessageBroker(RqueueMessageTemplate template) {
    this(template, null);
  }

  public RedisMessageBroker(
      RqueueMessageTemplate template, RedisMessageListenerContainer pubSubContainer) {
    if (template == null) {
      throw new IllegalArgumentException("template cannot be null");
    }
    this.template = template;
    this.pubSubContainer = pubSubContainer;
  }

  public RqueueMessageTemplate getTemplate() {
    return template;
  }

  @Override
  public void enqueue(QueueDetail q, RqueueMessage m) {
    template.addMessage(q.getQueueName(), m);
  }

  @Override
  public void enqueueWithDelay(QueueDetail q, RqueueMessage m, long delayMs) {
    // Delegate to existing scheduled-queue add path; processAt is encoded on the message.
    template.addMessageWithDelay(q.getScheduledQueueName(), q.getScheduledQueueChannelName(), m);
  }

  @Override
  public List<RqueueMessage> pop(QueueDetail q, String consumerName, int batch, Duration wait) {
    return template.pop(
        q.getQueueName(),
        q.getProcessingQueueName(),
        q.getProcessingQueueChannelName(),
        q.getVisibilityTimeout(),
        batch);
  }

  @Override
  public boolean ack(QueueDetail q, RqueueMessage m) {
    Long removed = template.removeElementFromZset(q.getProcessingQueueName(), m);
    return removed != null && removed > 0;
  }

  @Override
  public boolean nack(QueueDetail q, RqueueMessage m, long retryDelayMs) {
    if (retryDelayMs <= 0) {
      template.moveMessage(q.getProcessingQueueName(), q.getQueueName(), m, m);
    } else {
      template.moveMessageWithDelay(
          q.getProcessingQueueName(), q.getScheduledQueueName(), m, m, retryDelayMs);
    }
    return true;
  }

  @Override
  public long moveExpired(QueueDetail q, long now, int batch) {
    MessageMoveResult result =
        template.moveMessageZsetToList(q.getScheduledQueueName(), q.getQueueName(), batch);
    return result == null ? 0L : result.getNumberOfMessages();
  }

  @Override
  public List<RqueueMessage> peek(QueueDetail q, long offset, long count) {
    long end = (count <= 0) ? -1L : offset + count - 1;
    return template.readFromList(q.getQueueName(), offset, end);
  }

  @Override
  public long size(QueueDetail q) {
    RedisTemplate<String, RqueueMessage> rt = template.getTemplate();
    Long size = rt.opsForList().size(q.getQueueName());
    return size == null ? 0L : size;
  }

  @Override
  public AutoCloseable subscribe(String channel, Consumer<String> handler) {
    if (pubSubContainer == null) {
      throw new IllegalStateException(
          "RedisMessageListenerContainer not configured for RedisMessageBroker; subscribe is"
              + " unavailable");
    }
    final ChannelTopic topic = new ChannelTopic(channel);
    final MessageListener listener = new MessageListener() {
      @Override
      public void onMessage(Message message, byte[] pattern) {
        byte[] body = message.getBody();
        if (body == null) {
          return;
        }
        handler.accept(new String(body));
      }
    };
    pubSubContainer.addMessageListener(listener, topic);
    return () -> pubSubContainer.removeMessageListener(listener, topic);
  }

  @Override
  public void publish(String channel, String payload) {
    template.getTemplate().convertAndSend(channel, payload);
  }

  @Override
  public void parkForRetry(QueueDetail q, RqueueMessage old, RqueueMessage updated, long delayMs) {
    if (delayMs <= 0) {
      template.moveMessage(q.getProcessingQueueName(), q.getQueueName(), old, updated);
    } else {
      template.moveMessageWithDelay(
          q.getProcessingQueueName(), q.getScheduledQueueName(), old, updated, delayMs);
    }
  }

  @Override
  public void moveToDlq(
      QueueDetail source,
      String targetQueue,
      RqueueMessage old,
      RqueueMessage updated,
      long delayMs) {
    RedisUtils.executePipeLine(
        template.getTemplate(), (connection, keySerializer, valueSerializer) -> {
          byte[] updatedBytes = valueSerializer.serialize(updated);
          byte[] oldBytes = valueSerializer.serialize(old);
          byte[] processingQueueBytes = keySerializer.serialize(source.getProcessingQueueName());
          byte[] targetQueueBytes = keySerializer.serialize(targetQueue);
          if (delayMs > 0) {
            connection.zAdd(targetQueueBytes, delayMs, updatedBytes);
          } else {
            connection.lPush(targetQueueBytes, updatedBytes);
          }
          connection.zRem(processingQueueBytes, oldBytes);
        });
  }

  @Override
  public void scheduleNext(
      QueueDetail q, String messageKey, RqueueMessage message, long expirySeconds) {
    template.scheduleMessage(q.getScheduledQueueName(), messageKey, message, expirySeconds);
  }

  @Override
  public Long getVisibilityTimeoutScore(QueueDetail q, RqueueMessage m) {
    return template.getScore(q.getProcessingQueueName(), m);
  }

  @Override
  public boolean extendVisibilityTimeout(QueueDetail q, RqueueMessage m, long deltaMs) {
    return template.addScore(q.getProcessingQueueName(), m, deltaMs);
  }

  @Override
  public Capabilities capabilities() {
    return Capabilities.REDIS_DEFAULTS;
  }
}
