package com.github.sonus21.rqueue.core;

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

  public RqueueMessage getFromZset(String key) {
    Set<TypedTuple<RqueueMessage>> msgs = redisTemplate.opsForZSet().rangeWithScores(key, 0, 0);
    if (CollectionUtils.isEmpty(msgs)) {
      return null;
    }
    return msgs.stream().findFirst().get().getValue();
  }
}
