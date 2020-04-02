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

package rqueue.test;

import com.github.sonus21.rqueue.converter.GenericMessageConverter;
import com.github.sonus21.rqueue.core.RqueueMessage;
import com.github.sonus21.rqueue.utils.QueueUtility;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.messaging.Message;
import org.springframework.util.CollectionUtils;

@Slf4j
public abstract class Utility {
  private static final GenericMessageConverter converter = new GenericMessageConverter();

  public static RqueueMessage buildMessage(
      Object object, String queueName, Integer retryCount, Long delay) {
    Message<?> msg = converter.toMessage(object, null);
    return new RqueueMessage(queueName, (String) msg.getPayload(), retryCount, delay);
  }

  public static Map<String, List<RqueueMessage>> getMessageMap(
      String queueName, RedisTemplate<String, RqueueMessage> redisTemplate) {
    Map<String, List<RqueueMessage>> queueNameToMessage = new HashMap<>();

    List<RqueueMessage> messages = redisTemplate.opsForList().range(queueName, 0, -1);
    if (CollectionUtils.isEmpty(messages)) {
      messages = new ArrayList<>();
    }
    queueNameToMessage.put(queueName, messages);

    Set<RqueueMessage> messagesFromZset =
        redisTemplate.opsForZSet().range(QueueUtility.getTimeQueueName(queueName), 0, -1);
    if (!CollectionUtils.isEmpty(messagesFromZset)) {
      messages = new ArrayList<>(messagesFromZset);
    } else {
      messages = new ArrayList<>();
    }
    queueNameToMessage.put(QueueUtility.getTimeQueueName(queueName), messages);

    Set<RqueueMessage> messagesInProcessingQueue =
        redisTemplate.opsForZSet().range(QueueUtility.getProcessingQueueName(queueName), 0, -1);
    if (!CollectionUtils.isEmpty(messagesInProcessingQueue)) {
      messages = new ArrayList<>(messagesInProcessingQueue);
    } else {
      messages = new ArrayList<>();
    }
    queueNameToMessage.put(QueueUtility.getProcessingQueueName(queueName), messages);
    return queueNameToMessage;
  }

  public static void printQueueStats(
      List<String> queueNames, RedisTemplate<String, RqueueMessage> redisTemplate) {
    for (String queueName : queueNames) {
      for (Entry<String, List<RqueueMessage>> entry :
          Utility.getMessageMap(queueName, redisTemplate).entrySet()) {
        for (RqueueMessage message : entry.getValue()) {
          log.info("Queue: {} Msg: {}", entry.getKey(), message);
        }
      }
    }
  }
}
