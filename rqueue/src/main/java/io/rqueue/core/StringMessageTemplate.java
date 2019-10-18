package io.rqueue.core;

import java.util.concurrent.TimeUnit;
import org.springframework.data.redis.connection.RedisConnectionFactory;

public class StringMessageTemplate extends RqueueRedisTemplate<String, String> {

  public StringMessageTemplate(RedisConnectionFactory redisConnectionFactory) {
    super(redisConnectionFactory);
  }

  public boolean putIfAbsent(String key, Long time, TimeUnit timeUnit) {
    Boolean result = redisTemplate.opsForValue().setIfAbsent(key, key, time, timeUnit);
    if (result == null) {
      return false;
    }
    return result;
  }

  public void delete(String key) {
    redisTemplate.delete(key);
  }
}
