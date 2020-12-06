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
