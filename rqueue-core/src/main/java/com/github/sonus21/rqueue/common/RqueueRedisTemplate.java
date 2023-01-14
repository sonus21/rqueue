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

package com.github.sonus21.rqueue.common;

import com.github.sonus21.rqueue.utils.RedisUtils;
import java.io.Serializable;
import java.time.Duration;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.connection.DataType;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.ZSetOperations.TypedTuple;

@Slf4j
public class RqueueRedisTemplate<V extends Serializable> {

  protected RedisTemplate<String, V> redisTemplate;

  public RqueueRedisTemplate(RedisConnectionFactory redisConnectionFactory) {
    this.redisTemplate = RedisUtils.getRedisTemplate(redisConnectionFactory);
    this.redisTemplate.afterPropertiesSet();
  }

  public RedisTemplate<String, V> getRedisTemplate() {
    return this.redisTemplate;
  }

  public Long removeFromZset(String zsetName, V val) {
    return redisTemplate.opsForZSet().remove(zsetName, val);
  }

  public Long getListSize(String lName) {
    return redisTemplate.opsForList().size(lName);
  }

  public Long getZsetSize(String zsetName) {
    return redisTemplate.opsForZSet().size(zsetName);
  }

  public Double getZsetMemberScore(String zsetName, String key) {
    return redisTemplate.opsForZSet().score(zsetName, key);
  }

  public Long rpush(String listName, V val) {
    return redisTemplate.opsForList().rightPush(listName, val);
  }

  public Long addToSet(String setName, V... values) {
    return redisTemplate.opsForSet().add(setName, values);
  }

  public void set(String key, V val) {
    redisTemplate.opsForValue().set(key, val);
  }

  public V get(String key) {
    return redisTemplate.opsForValue().get(key);
  }

  public boolean exist(String key) {
    return ttl(key) != -2;
  }

  public int ttl(String key) {
    return redisTemplate.getExpire(key, TimeUnit.SECONDS).intValue();
  }

  public List<V> mget(Collection<String> keys) {
    return redisTemplate.opsForValue().multiGet(keys);
  }

  public void mset(Map<String, V> map) {
    redisTemplate.opsForValue().multiSet(map);
  }

  public void set(String key, V val, Duration duration) {
    redisTemplate.opsForValue().set(key, val, duration.toMillis(), TimeUnit.MILLISECONDS);
  }

  public Boolean setIfAbsent(String lockKey, V val, Duration duration) {
    boolean result = redisTemplate.opsForValue().setIfAbsent(lockKey, val);
    if (result) {
      redisTemplate.expire(lockKey, duration.toMillis(), TimeUnit.MILLISECONDS);
    }
    return result;
  }

  public Boolean delete(String key) {
    return redisTemplate.delete(key);
  }

  public Object delete(Collection<String> keys) {
    return redisTemplate.delete(keys);
  }

  public DataType type(String key) {
    return redisTemplate.type(key);
  }

  public List<V> lrange(String key, long start, long end) {
    return redisTemplate.opsForList().range(key, start, end);
  }

  public Set<V> zrange(String key, long start, long end) {
    return redisTemplate.opsForZSet().range(key, start, end);
  }

  public Set<TypedTuple<V>> zrangeWithScore(String key, long start, long end) {
    return redisTemplate.opsForZSet().rangeWithScores(key, start, end);
  }

  public void zremRangeByScore(String key, long min, long max) {
    redisTemplate.opsForZSet().removeRangeByScore(key, min, max);
  }

  public Set<V> getMembers(String key) {
    return redisTemplate.opsForSet().members(key);
  }

  public void ltrim(String key, Integer start, Integer end) {
    redisTemplate.opsForList().trim(key, start, end);
  }

  public Boolean zadd(String key, V val, long score) {
    return redisTemplate.opsForZSet().add(key, val, score);
  }

  public void rename(String oldKey, String newKey) {
    rename(Collections.singletonList(oldKey), Collections.singletonList(newKey));
  }

  public void rename(List<String> oldKeys, List<String> newKeys) {
    if (oldKeys.size() != newKeys.size()) {
      throw new IllegalArgumentException("Old key and new key space set is different");
    }
    List<String> srcKeys = new LinkedList<>();
    List<String> dstKeys = new LinkedList<>();
    for (int i = 0; i < oldKeys.size(); i++) {
      String key = oldKeys.get(i);
      if (exist(key)) {
        srcKeys.add(key);
        dstKeys.add(newKeys.get(i));
      }
    }
    log.debug(
        "Pipeline result: {}",
        RedisUtils.executePipeLine(
            redisTemplate,
            (connection, keySerializer, valueSerializer) -> {
              for (int i = 0; i < srcKeys.size(); i++) {
                // TODO fix cross slot error?
                byte[] src = keySerializer.serialize(srcKeys.get(i));
                byte[] dst = keySerializer.serialize(dstKeys.get(i));
                connection.rename(src, dst);
              }
            }));
    if (srcKeys.size() != oldKeys.size()) {
      List<String> diff = new LinkedList<>(oldKeys);
      diff.removeAll(srcKeys);
      log.info("Some keys does not exist diff: {}", diff);
    }
  }
}
