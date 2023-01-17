/*
 * Copyright (c) 2020-2023 Sonu Kumar
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

package com.github.sonus21.rqueue.utils;

import com.github.sonus21.TestBase;
import com.github.sonus21.rqueue.core.RqueueMessageTemplate;
import com.github.sonus21.rqueue.core.impl.RqueueMessageTemplateImpl;
import com.github.sonus21.rqueue.listener.QueueDetail;
import com.github.sonus21.rqueue.models.Concurrency;
import com.github.sonus21.rqueue.models.db.QueueConfig;
import java.util.HashMap;
import java.util.Map;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.script.DefaultScriptExecutor;

@NoArgsConstructor(access = AccessLevel.PRIVATE)
public final class TestUtils extends TestBase {

  public static RqueueMessageTemplate rqueueMessageTemplate(
      RedisConnectionFactory redisConnectionFactory,
      RedisTemplate<String, ?> redisTemplate, DefaultScriptExecutor<String> scriptExecutor)
      throws IllegalAccessException {
    RqueueMessageTemplate rqueueMessageTemplate = new RqueueMessageTemplateImpl(
        redisConnectionFactory, null);
    FieldUtils.writeField(rqueueMessageTemplate, "redisTemplate", redisTemplate, true);
    if (scriptExecutor != null) {
      FieldUtils.writeField(rqueueMessageTemplate, "scriptExecutor", scriptExecutor, true);
    }
    return rqueueMessageTemplate;
  }

  public static QueueConfig createQueueConfig(
      String name, int numRetry, long visibilityTimeout, String dlq) {
    QueueConfig queueConfig = createQueueDetail(name, numRetry, visibilityTimeout, dlq).toConfig();
    queueConfig.setId(getQueueConfigKey(name));
    return queueConfig;
  }

  public static QueueConfig createQueueConfig(String name) {
    return createQueueConfig(name, null);
  }

  public static QueueConfig createQueueConfig(String name, String dlq) {
    return createQueueConfig(name, 3, 900000L, dlq);
  }

  public static QueueDetail createQueueDetail(String name) {
    return createQueueDetail(name, 3, 900000L, null);
  }

  public static QueueDetail createQueueDetail(
      String name, int numRetry, long visibilityTimeout, String dlq) {
    return createQueueDetail(name, new HashMap<>(), numRetry, visibilityTimeout, dlq);
  }

  public static QueueDetail createQueueDetail(
      String name,
      Map<String, Integer> priority,
      int numRetry,
      long visibilityTimeout,
      String dlq) {
    return QueueDetail.builder()
        .name(name)
        .queueName("__rq::queue::" + name)
        .processingQueueName("__rq::p-queue::" + name)
        .processingQueueChannelName("__rq::p-channel::" + name)
        .scheduledQueueName("__rq::d-queue::" + name)
        .scheduledQueueChannelName("__rq::d-channel::" + name)
        .completedQueueName("__rq::c-queue::" + name)
        .numRetry(numRetry)
        .visibilityTimeout(visibilityTimeout)
        .deadLetterQueueName(dlq)
        .priority(priority)
        .priorityGroup("")
        .concurrency(new Concurrency(-1, -1))
        .active(true)
        .build();
  }

  public static String getQueueConfigKey(String name) {
    return "__rq::q-config::" + name;
  }

  public static String getQueuesKey() {
    return "__rq::queues";
  }

  public static QueueDetail createQueueDetail(String name, long visibilityTimeout) {
    return createQueueDetail(name, 3, visibilityTimeout, null);
  }

  public static QueueDetail createQueueDetail(String name, String dlq) {
    return createQueueDetail(name, 3, 900000L, dlq);
  }

  public static QueueDetail createQueueDetail(String name, long visibilityTimeout, String dlq) {
    return createQueueDetail(name, 3, visibilityTimeout, dlq);
  }
}
