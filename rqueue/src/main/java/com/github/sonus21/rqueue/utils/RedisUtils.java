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

package com.github.sonus21.rqueue.utils;

import com.github.sonus21.rqueue.converter.RqueueRedisSerializer;
import java.util.List;
import org.springframework.data.redis.connection.RedisConnection;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.connection.ReturnType;
import org.springframework.data.redis.core.RedisCallback;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.serializer.StringRedisSerializer;

public class RedisUtils {
  private RedisUtils() {}

  public static <V> RedisTemplate<String, V> getRedisTemplate(
      RedisConnectionFactory redisConnectionFactory) {
    RedisTemplate<String, V> redisTemplate = new RedisTemplate<>();
    StringRedisSerializer stringRedisSerializer = new StringRedisSerializer();
    RqueueRedisSerializer rqueueRedisSerializer = new RqueueRedisSerializer();
    redisTemplate.setConnectionFactory(redisConnectionFactory);
    redisTemplate.setKeySerializer(stringRedisSerializer);
    redisTemplate.setValueSerializer(rqueueRedisSerializer);
    redisTemplate.setHashKeySerializer(stringRedisSerializer);
    redisTemplate.setHashValueSerializer(rqueueRedisSerializer);
    return redisTemplate;
  }

  public static <V> List<Object> executePipeLine(
      RedisTemplate<String, V> template, RedisPipelineCallback callback) {
    return template.executePipelined(
        (RedisCallback<Object>)
            connection -> {
              RqueueRedisSerializer valueSerializer =
                  (RqueueRedisSerializer) template.getValueSerializer();
              StringRedisSerializer keySerializer =
                  (StringRedisSerializer) template.getKeySerializer();
              callback.doInRedis(connection, keySerializer, valueSerializer);
              return null;
            });
  }

  public interface RedisPipelineCallback {
    void doInRedis(
        RedisConnection connection,
        StringRedisSerializer keySerializer,
        RqueueRedisSerializer valueSerializer);
  }

  public static int updateAndGetVersion(
      RedisConnectionFactory redisConnectionFactory, String versionDbKey, int defaultVersion) {
    RedisConnection connection = redisConnectionFactory.getConnection();
    byte[] versionKey = versionDbKey.getBytes();
    byte[] versionFromDb = connection.get(versionKey);
    if (SerializationUtils.isEmpty(versionFromDb)) {
      Long count =
          connection.eval(
              "return #redis.pcall('keys', 'rqueue-*')".getBytes(), ReturnType.INTEGER, 0);
      if (count != null && count > 0L) {
        int version = 1;
        connection.set(versionKey, String.valueOf(version).getBytes());
        return version;
      }
      connection.set(versionKey, String.valueOf(defaultVersion).getBytes());
      return defaultVersion;
    }
    return Integer.parseInt(new String(versionFromDb));
  }
}
