package com.github.sonus21.rqueue.converter;

import com.github.sonus21.rqueue.utils.SerializationUtils;
import org.springframework.data.redis.serializer.GenericJackson2JsonRedisSerializer;
import org.springframework.data.redis.serializer.RedisSerializer;
import org.springframework.data.redis.serializer.SerializationException;

public class RqueueRedisSerializer implements RedisSerializer<Object> {
  private GenericJackson2JsonRedisSerializer jackson2JsonRedisSerializer =
      new GenericJackson2JsonRedisSerializer();

  @Override
  public byte[] serialize(Object t) throws SerializationException {
    return jackson2JsonRedisSerializer.serialize(t);
  }

  @Override
  public Object deserialize(byte[] bytes) throws SerializationException {
    if (SerializationUtils.isEmpty(bytes)) {
      return null;
    }
    try {
      return jackson2JsonRedisSerializer.deserialize(bytes);
    } catch (Exception e) {
      return new String(bytes);
    }
  }
}
