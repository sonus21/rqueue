/*
 * Copyright (c)  2019-2019, Sonu Kumar
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *       https://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */

package com.github.sonus21.rqueue.core;

import static com.github.sonus21.rqueue.utils.QueueInfo.getChannelName;
import static com.github.sonus21.rqueue.utils.QueueInfo.getProcessingQueueChannelName;
import static com.github.sonus21.rqueue.utils.QueueInfo.getProcessingQueueName;
import static com.github.sonus21.rqueue.utils.QueueInfo.getTimeQueueName;

import com.github.sonus21.rqueue.utils.QueueInfo;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.Resource;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.core.script.DefaultScriptExecutor;
import org.springframework.data.redis.core.script.RedisScript;
import org.springframework.util.CollectionUtils;

public class RqueueMessageTemplate extends RqueueRedisTemplate<String, RqueueMessage> {
  private Resource addMessage = new ClassPathResource("scripts/add-message.lua");
  private Resource removeMessage = new ClassPathResource("scripts/remove-message.lua");
  private Resource removeAndAddMessage = new ClassPathResource("scripts/add-remove-message.lua");
  private RedisScript<Long> addScript = RedisScript.of(addMessage, Long.class);
  private RedisScript<Long> removeAdnMessageScript =
      RedisScript.of(removeAndAddMessage, Long.class);
  private RedisScript<RqueueMessage> removeScript =
      RedisScript.of(removeMessage, RqueueMessage.class);
  private DefaultScriptExecutor<String> scriptExecutor;

  public RqueueMessageTemplate(RedisConnectionFactory redisConnectionFactory) {
    super(redisConnectionFactory);
    this.scriptExecutor = new DefaultScriptExecutor<>(redisTemplate);
  }

  public void add(String queueName, RqueueMessage message) {
    redisTemplate.opsForList().rightPush(queueName, message);
  }

  public RqueueMessage pop(String queueName) {
    long currentTime = System.currentTimeMillis();
    return scriptExecutor.execute(
        removeScript,
        Arrays.asList(
            queueName, getProcessingQueueName(queueName), getProcessingQueueChannelName(queueName)),
        currentTime,
        QueueInfo.getMessageReEnqueueTime(currentTime));
  }

  public void addWithDelay(String queueName, RqueueMessage rqueueMessage) {
    scriptExecutor.execute(
        addScript,
        Arrays.asList(getTimeQueueName(queueName), getChannelName(queueName)),
        rqueueMessage,
        rqueueMessage.getProcessAt(),
        rqueueMessage.getQueuedTime());
  }

  public void removeFromZset(String zsetName, RqueueMessage rqueueMessage) {
    redisTemplate.opsForZSet().remove(zsetName, rqueueMessage);
  }

  public void replaceMessage(String zsetName, RqueueMessage src, RqueueMessage tgt) {
    scriptExecutor.execute(removeAdnMessageScript, Collections.singletonList(zsetName), src, tgt);
  }

  public List<RqueueMessage> getAllMessages(String queueName) {
    List<RqueueMessage> messages = redisTemplate.opsForList().range(queueName, 0, -1);
    if (CollectionUtils.isEmpty(messages)) {
      messages = new ArrayList<>();
    }
    Set<RqueueMessage> messagesFromZset =
        redisTemplate.opsForZSet().range(QueueInfo.getTimeQueueName(queueName), 0, -1);
    if (!CollectionUtils.isEmpty(messagesFromZset)) {
      messages.addAll(messagesFromZset);
    }
    Set<RqueueMessage> messagesInProcessingQueue =
        redisTemplate.opsForZSet().range(QueueInfo.getProcessingQueueName(queueName), 0, -1);
    if (!CollectionUtils.isEmpty(messagesInProcessingQueue)) {
      messages.addAll(messagesInProcessingQueue);
    }
    return messages;
  }
}
