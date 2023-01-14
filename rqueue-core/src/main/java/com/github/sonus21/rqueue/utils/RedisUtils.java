/*
 * Copyright (c) 2019-2023 Sonu Kumar
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

import com.github.sonus21.rqueue.common.RqueueRedisTemplate;
import com.github.sonus21.rqueue.converter.RqueueRedisSerializer;
import java.util.List;
import org.springframework.data.redis.connection.ReactiveRedisConnectionFactory;
import org.springframework.data.redis.connection.RedisConnection;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.connection.ReturnType;
import org.springframework.data.redis.core.ReactiveRedisTemplate;
import org.springframework.data.redis.core.RedisCallback;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.serializer.RedisSerializationContext;
import org.springframework.data.redis.serializer.RedisSerializationContext.RedisSerializationContextBuilder;
import org.springframework.data.redis.serializer.RedisSerializer;
import org.springframework.data.redis.serializer.StringRedisSerializer;

public final class RedisUtils {

  @SuppressWarnings({"java:S1104", "java:S1444"})
  public static RedisTemplateProvider redisTemplateProvider =
      new RedisTemplateProvider() {
        @Override
        public <V> RedisTemplate<String, V> getRedisTemplate(
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
      };

  @SuppressWarnings({"java:S1104", "java:S1444"})
  public static RedisSerializationContextProvider redisSerializationContextProvider =
      () -> {
        StringRedisSerializer stringRedisSerializer = new StringRedisSerializer();
        RqueueRedisSerializer rqueueRedisSerializer = new RqueueRedisSerializer();
        RedisSerializationContextBuilder<String, Object> redisSerializationContextBuilder =
            RedisSerializationContext.newSerializationContext();
        redisSerializationContextBuilder =
            redisSerializationContextBuilder.key(stringRedisSerializer);
        redisSerializationContextBuilder =
            redisSerializationContextBuilder.value(rqueueRedisSerializer);
        redisSerializationContextBuilder =
            redisSerializationContextBuilder.hashKey(stringRedisSerializer);
        redisSerializationContextBuilder =
            redisSerializationContextBuilder.hashValue(rqueueRedisSerializer);
        return redisSerializationContextBuilder.build();
      };

  @SuppressWarnings({"java:S1104", "java:S1444"})
  public static ReactiveRedisTemplateProvider reactiveRedisTemplateProvider =
      new ReactiveRedisTemplateProvider() {
        @Override
        public <V> ReactiveRedisTemplate<String, V> getRedisTemplate(
            ReactiveRedisConnectionFactory redisConnectionFactory) {
          return new ReactiveRedisTemplate(
              redisConnectionFactory, redisSerializationContextProvider.getSerializationContext());
        }
      };

  private RedisUtils() {
  }

  public static <V> RedisTemplate<String, V> getRedisTemplate(
      RedisConnectionFactory redisConnectionFactory) {
    return redisTemplateProvider.getRedisTemplate(redisConnectionFactory);
  }

  public static <V> ReactiveRedisTemplate<String, V> getReactiveRedisTemplate(
      ReactiveRedisConnectionFactory redisConnectionFactory) {
    return reactiveRedisTemplateProvider.getRedisTemplate(redisConnectionFactory);
  }

  @SuppressWarnings("unchecked")
  public static <V> List<Object> executePipeLine(
      RedisTemplate<String, V> template, RedisPipelineCallback callback) {
    return template.executePipelined(
        (RedisCallback<Object>)
            connection -> {
              RedisSerializer<String> keySerializer =
                  (RedisSerializer<String>) template.getKeySerializer();
              RedisSerializer<Object> valueSerializer =
                  (RedisSerializer<Object>) template.getValueSerializer();
              callback.doInRedis(connection, keySerializer, valueSerializer);
              return null;
            });
  }

  public static void setVersion(
      RqueueRedisTemplate<Integer> rqueueRedisTemplate, String versionKey, int version) {
    rqueueRedisTemplate.set(versionKey, version);
  }

  private static int checkDbVersion(Object data) {
    if (data instanceof Integer || data instanceof Long) {
      return ((Number) data).intValue();
    } else if (data instanceof String) {
      return Integer.parseInt((String) data);
    } else if (data != null) {
      throw new IllegalStateException("Invalid db version" + data);
    }
    return -1;
  }

  public static int updateAndGetVersion(
      RqueueRedisTemplate<Integer> rqueueRedisTemplate, String versionKey, int defaultVersion) {
    Object data = rqueueRedisTemplate.get(versionKey);
    int dbVersion = checkDbVersion(data);
    if (dbVersion > 0) {
      return dbVersion;
    }
    List<Object> result =
        RedisUtils.executePipeLine(
            rqueueRedisTemplate.getRedisTemplate(),
            ((connection, keySerializer, valueSerializer) ->
                connection.eval(
                    "return #redis.pcall('keys', 'rqueue-*')".getBytes(), ReturnType.INTEGER, 0)));
    Long count = (Long) result.get(0);
    if (count != null && count > 0L) {
      rqueueRedisTemplate.set(versionKey, 1);
      return 1;
    }
    rqueueRedisTemplate.set(versionKey, defaultVersion);
    return defaultVersion;
  }

  public interface RedisTemplateProvider {

    <V> RedisTemplate<String, V> getRedisTemplate(RedisConnectionFactory redisConnectionFactory);
  }

  public interface ReactiveRedisTemplateProvider {

    <V> ReactiveRedisTemplate<String, V> getRedisTemplate(
        ReactiveRedisConnectionFactory redisConnectionFactory);
  }

  public interface RedisSerializationContextProvider {

    RedisSerializationContext<String, Object> getSerializationContext();
  }

  public interface RedisPipelineCallback {

    void doInRedis(
        RedisConnection connection,
        RedisSerializer<String> keySerializer,
        RedisSerializer<Object> valueSerializer);
  }
}
