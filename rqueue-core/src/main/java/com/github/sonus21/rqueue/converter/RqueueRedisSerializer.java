package com.github.sonus21.rqueue.converter;

import com.github.sonus21.rqueue.utils.SerializationUtils;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.serializer.GenericJackson2JsonRedisSerializer;
import org.springframework.data.redis.serializer.RedisSerializer;
import org.springframework.data.redis.serializer.SerializationException;

@Slf4j
public class RqueueRedisSerializer implements RedisSerializer<Object> {
  private final RedisSerializer<Object> serializer;

  public RqueueRedisSerializer(RedisSerializer<Object> redisSerializer) {
    this.serializer = redisSerializer;
  }

  public RqueueRedisSerializer() {
    this(new GenericJackson2JsonRedisSerializer());
  }

  @Override
  public byte[] serialize(Object t) throws SerializationException {
    return serializer.serialize(t);
  }

  @Override
  public Object deserialize(byte[] bytes) throws SerializationException {
    if (SerializationUtils.isEmpty(bytes)) {
      return null;
    }
    try {
      return serializer.deserialize(bytes);
    } catch (Exception e) {
      log.warn("Jackson deserialization has failed {}", new String(bytes), e);
      return new String(bytes);
    }
  }
}
