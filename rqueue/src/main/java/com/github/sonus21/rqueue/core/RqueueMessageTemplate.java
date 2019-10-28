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

import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.core.ZSetOperations.TypedTuple;
import org.springframework.util.CollectionUtils;

public class RqueueMessageTemplate extends RqueueRedisTemplate<String, RqueueMessage> {
  private static final long KEY_POP_TIMEOUT = 10;

  public RqueueMessageTemplate(RedisConnectionFactory redisConnectionFactory) {
    super(redisConnectionFactory);
  }

  public void add(String key, RqueueMessage message) {
    redisTemplate.opsForList().rightPush(key, message);
  }

  public RqueueMessage lpop(String key) {
    return redisTemplate.opsForList().leftPop(key, KEY_POP_TIMEOUT, TimeUnit.SECONDS);
  }

  public void addToZset(String key, RqueueMessage rqueueMessage) {
    redisTemplate.opsForZSet().add(key, rqueueMessage, rqueueMessage.getProcessAt());
  }

  public void removeFromZset(String key, RqueueMessage rqueueMessage) {
    redisTemplate.opsForZSet().remove(key, rqueueMessage);
  }

  public RqueueMessage getFirstFromZset(String key) {
    Set<TypedTuple<RqueueMessage>> msgs = redisTemplate.opsForZSet().rangeWithScores(key, 0, 0);
    if (CollectionUtils.isEmpty(msgs)) {
      return null;
    }
    Optional<TypedTuple<RqueueMessage>> element = msgs.stream().findFirst();
    return element.map(TypedTuple::getValue).orElse(null);
  }
}
