package io.rqueue.core;

import java.io.Serializable;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.serializer.GenericJackson2JsonRedisSerializer;
import org.springframework.data.redis.serializer.StringRedisSerializer;

abstract class RqueueRedisTemplate<K, V extends Serializable> {
  protected RedisTemplate<K, V> redisTemplate;

  RqueueRedisTemplate(RedisConnectionFactory redisConnectionFactory) {
    this.redisTemplate = new RedisTemplate<>();
    this.redisTemplate.setConnectionFactory(redisConnectionFactory);
    this.redisTemplate.setKeySerializer(new StringRedisSerializer());
    this.redisTemplate.setValueSerializer(new GenericJackson2JsonRedisSerializer());
    this.redisTemplate.setHashKeySerializer(new StringRedisSerializer());
    this.redisTemplate.setHashValueSerializer(new GenericJackson2JsonRedisSerializer());
    this.redisTemplate.afterPropertiesSet();
  }
}
