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

package com.github.sonus21.rqueue.core.impl;

import static com.github.sonus21.rqueue.core.RedisScriptFactory.getScript;

import com.github.sonus21.rqueue.common.RqueueRedisTemplate;
import com.github.sonus21.rqueue.core.RedisScriptFactory.ScriptType;
import com.github.sonus21.rqueue.core.RqueueMessage;
import com.github.sonus21.rqueue.core.RqueueMessageTemplate;
import com.github.sonus21.rqueue.models.MessageMoveResult;
import com.github.sonus21.rqueue.utils.Constants;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.ZSetOperations.TypedTuple;
import org.springframework.data.redis.core.script.DefaultScriptExecutor;
import org.springframework.data.redis.core.script.RedisScript;
import org.springframework.util.CollectionUtils;

/**
 * RqueueMessageTemplate is the core of the Rqueue, this deals with Redis calls.
 *
 * <p>It communicates with the Redis using Lua script and direct calls.
 */
@SuppressWarnings("unchecked")
@Slf4j
public class RqueueMessageTemplateImpl extends RqueueRedisTemplate<RqueueMessage>
    implements RqueueMessageTemplate {
  private final DefaultScriptExecutor<String> scriptExecutor;

  public RqueueMessageTemplateImpl(RedisConnectionFactory redisConnectionFactory) {
    super(redisConnectionFactory);
    scriptExecutor = new DefaultScriptExecutor<>(redisTemplate);
  }

  @Override
  public RqueueMessage pop(
      String queueName,
      String processingQueueName,
      String processingChannelName,
      long visibilityTimeout) {
    long currentTime = System.currentTimeMillis();
    RedisScript<RqueueMessage> script =
        (RedisScript<RqueueMessage>) getScript(ScriptType.DEQUEUE_MESSAGE);
    return scriptExecutor.execute(
        script,
        Arrays.asList(queueName, processingQueueName, processingChannelName),
        currentTime,
        currentTime + visibilityTimeout);
  }

  @Override
  public Long addMessageWithDelay(
      String delayQueueName, String delayQueueChannelName, RqueueMessage rqueueMessage) {
    RedisScript<Long> script = (RedisScript<Long>) getScript(ScriptType.ENQUEUE_MESSAGE);
    return scriptExecutor.execute(
        script,
        Arrays.asList(delayQueueName, delayQueueChannelName),
        rqueueMessage,
        rqueueMessage.getProcessAt(),
        System.currentTimeMillis());
  }

  @Override
  public Long addMessage(String listName, RqueueMessage rqueueMessage) {
    return rpush(listName, rqueueMessage);
  }

  @Override
  public Boolean addToZset(String zsetName, RqueueMessage rqueueMessage, long score) {
    return zadd(zsetName, rqueueMessage, score);
  }

  @Override
  public void moveMessage(
      String srcZsetName, String tgtZsetName, RqueueMessage src, RqueueMessage tgt, long delay) {
    RedisScript<Long> script = (RedisScript<Long>) getScript(ScriptType.MOVE_MESSAGE);
    Long response =
        scriptExecutor.execute(
            script,
            Arrays.asList(srcZsetName, tgtZsetName),
            src,
            tgt,
            System.currentTimeMillis() + delay);
    if (response == null) {
      log.error("Duplicate processing for the message {}", src);
    }
  }

  @Override
  public List<RqueueMessage> getAllMessages(
      String queueName, String processingQueueName, String delayQueueName) {
    List<RqueueMessage> messages = lrange(queueName, 0, -1);
    if (CollectionUtils.isEmpty(messages)) {
      messages = new ArrayList<>();
    }
    Set<RqueueMessage> messagesInProcessingQueue = zrange(processingQueueName, 0, -1);
    if (!CollectionUtils.isEmpty(messagesInProcessingQueue)) {
      messages.addAll(messagesInProcessingQueue);
    }
    if (delayQueueName != null) {
      Set<RqueueMessage> messagesFromZset = zrange(delayQueueName, 0, -1);
      if (!CollectionUtils.isEmpty(messagesFromZset)) {
        messages.addAll(messagesFromZset);
      }
    }
    return messages;
  }

  private MessageMoveResult moveMessageToList(
      String src, String dst, int maxMessage, ScriptType scriptType) {
    RedisScript<Long> script = (RedisScript<Long>) getScript(scriptType);
    long messagesInSrc = maxMessage;
    int remainingMessages = maxMessage;
    while (messagesInSrc > 0 && remainingMessages > 0) {
      long messageCount = Math.min(remainingMessages, Constants.MAX_MESSAGES);
      messagesInSrc = scriptExecutor.execute(script, Arrays.asList(src, dst), messageCount);
      remainingMessages -= messageCount;
    }
    return new MessageMoveResult(maxMessage - remainingMessages, true);
  }

  @Override
  public MessageMoveResult moveMessageListToList(
      String srcQueueName, String dstQueueName, int numberOfMessage) {
    return moveMessageToList(
        srcQueueName, dstQueueName, numberOfMessage, ScriptType.MOVE_MESSAGE_LIST_TO_LIST);
  }

  @Override
  public MessageMoveResult moveMessageZsetToList(
      String sourceZset, String destinationList, int maxMessage) {
    return moveMessageToList(
        sourceZset, destinationList, maxMessage, ScriptType.MOVE_MESSAGE_ZSET_TO_LIST);
  }

  @Override
  public MessageMoveResult moveMessageListToZset(
      String sourceList, String destinationZset, int maxMessage, long score) {
    RedisScript<Long> script = (RedisScript<Long>) getScript(ScriptType.MOVE_MESSAGE_LIST_TO_ZSET);
    long messagesInList = maxMessage;
    int remainingMessages = maxMessage;
    while (messagesInList > 0 && remainingMessages > 0) {
      long messageCount = Math.min(remainingMessages, Constants.MAX_MESSAGES);
      messagesInList =
          scriptExecutor.execute(
              script, Arrays.asList(sourceList, destinationZset), messageCount, score);
      remainingMessages -= messageCount;
    }
    return new MessageMoveResult(maxMessage - remainingMessages, true);
  }

  @Override
  public MessageMoveResult moveMessageZsetToZset(
      String sourceZset,
      String destinationZset,
      int maxMessage,
      long newScore,
      boolean fixedScore) {
    RedisScript<Long> script = (RedisScript<Long>) getScript(ScriptType.MOVE_MESSAGE_ZSET_TO_ZSET);
    long messageInZset = maxMessage;
    int remainingMessages = maxMessage;
    while (messageInZset > 0 && remainingMessages > 0) {
      long messageCount = Math.min(remainingMessages, Constants.MAX_MESSAGES);
      messageInZset =
          scriptExecutor.execute(
              script,
              Arrays.asList(sourceZset, destinationZset),
              messageCount,
              newScore,
              fixedScore);
      remainingMessages -= messageCount;
    }
    return new MessageMoveResult(maxMessage - remainingMessages, true);
  }

  @Override
  public List<RqueueMessage> readFromZset(String name, long start, long end) {
    Set<RqueueMessage> messages = zrange(name, start, end);
    if (messages == null) {
      return new ArrayList<>();
    }
    return new ArrayList<>(messages);
  }

  @Override
  public List<TypedTuple<RqueueMessage>> readFromZsetWithScore(String name, long start, long end) {
    Set<TypedTuple<RqueueMessage>> messages = zrangeWithScore(name, start, end);
    if (messages == null) {
      return new ArrayList<>();
    }
    return new ArrayList<>(messages);
  }

  @Override
  public Long getScore(String delayedQueueName, RqueueMessage rqueueMessage) {
    Double score = redisTemplate.opsForZSet().score(delayedQueueName, rqueueMessage);
    if (score == null) {
      return null;
    }
    return score.longValue();
  }

  @Override
  public Long scheduleMessage(
      String zsetName, String messageId, RqueueMessage rqueueMessage, long expiryInMilliSeconds) {
    RedisScript<Long> script = (RedisScript<Long>) getScript(ScriptType.SCHEDULE_MESSAGE);
    return scriptExecutor.execute(
        script,
        Arrays.asList(messageId, zsetName),
        expiryInMilliSeconds,
        rqueueMessage,
        rqueueMessage.getProcessAt());
  }

  @Override
  public List<RqueueMessage> readFromList(String name, long start, long end) {
    List<RqueueMessage> messages = lrange(name, start, end);
    if (messages == null) {
      return new ArrayList<>();
    }
    return messages;
  }

  @Override
  public RedisTemplate<String, RqueueMessage> getTemplate() {
    return super.redisTemplate;
  }

  @Override
  public Long removeElementFromZset(String zsetName, RqueueMessage rqueueMessage) {
    return super.removeFromZset(zsetName, rqueueMessage);
  }
}
