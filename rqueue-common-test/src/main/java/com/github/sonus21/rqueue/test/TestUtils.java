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

package com.github.sonus21.rqueue.test;

import com.github.sonus21.rqueue.config.RqueueConfig;
import com.github.sonus21.rqueue.core.RqueueMessage;
import com.github.sonus21.rqueue.core.RqueueMessageTemplate;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.reflect.FieldUtils;

@Slf4j
public class TestUtils {

  private TestUtils() {}

  public static Map<String, List<RqueueMessage>> getMessageMap(
      RqueueConfig rqueueConfig, String queueName, RqueueMessageTemplate redisTemplate) {
    Map<String, List<RqueueMessage>> queueNameToMessage = new HashMap<>();

    List<RqueueMessage> messages =
        redisTemplate.readFromList(rqueueConfig.getQueueName(queueName), 0, -1);
    queueNameToMessage.put(rqueueConfig.getQueueName(queueName), messages);

    List<RqueueMessage> messagesFromZset =
        redisTemplate.readFromZset(rqueueConfig.getDelayedQueueName(queueName), 0, -1);
    queueNameToMessage.put(rqueueConfig.getDelayedQueueName(queueName), messagesFromZset);

    List<RqueueMessage> messagesInProcessingQueue =
        redisTemplate.readFromZset(rqueueConfig.getProcessingQueueName(queueName), 0, -1);
    queueNameToMessage.put(
        rqueueConfig.getProcessingQueueName(queueName), messagesInProcessingQueue);
    return queueNameToMessage;
  }

  public static void printQueueStats(
      RqueueConfig rqueueConfig, List<String> queueNames, RqueueMessageTemplate redisTemplate) {
    for (String queueName : queueNames) {
      for (Entry<String, List<RqueueMessage>> entry :
          TestUtils.getMessageMap(rqueueConfig, queueName, redisTemplate).entrySet()) {
        for (RqueueMessage message : entry.getValue()) {
          log.info("Queue: {} Msg: {}", entry.getKey(), message);
        }
      }
    }
  }

  public static void writeField(Object tgt, String fieldName, Object val)
      throws IllegalAccessException {
    FieldUtils.writeField(tgt, fieldName, val, true);
  }

  public static Object readField(Object tgt, String fieldName) throws IllegalAccessException {
    return FieldUtils.readField(tgt, fieldName, true);
  }
}
