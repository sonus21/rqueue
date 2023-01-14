/*
 * Copyright (c) 2021-2023 Sonu Kumar
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

package com.github.sonus21.rqueue.dao.impl;

import com.github.sonus21.rqueue.common.RqueueRedisTemplate;
import com.github.sonus21.rqueue.config.RqueueConfig;
import com.github.sonus21.rqueue.core.RedisScriptFactory;
import com.github.sonus21.rqueue.core.RedisScriptFactory.ScriptType;
import com.github.sonus21.rqueue.dao.RqueueStringDao;
import com.github.sonus21.rqueue.utils.RedisUtils;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Set;
import org.springframework.data.redis.connection.DataType;
import org.springframework.data.redis.core.ZSetOperations.TypedTuple;
import org.springframework.data.redis.core.script.DefaultScriptExecutor;
import org.springframework.data.redis.core.script.RedisScript;
import org.springframework.util.CollectionUtils;

public class RqueueStringDaoImpl implements RqueueStringDao {

  private final RqueueRedisTemplate<String> redisTemplate;
  private final RedisScript<Boolean> delIfSameScript;
  private final DefaultScriptExecutor<String> scriptExecutor;

  public RqueueStringDaoImpl(RqueueConfig rqueueConfig) {
    this.redisTemplate = new RqueueRedisTemplate<>(rqueueConfig.getConnectionFactory());
    this.delIfSameScript = RedisScriptFactory.getScript(ScriptType.DELETE_IF_SAME);
    this.scriptExecutor = new DefaultScriptExecutor<>(this.redisTemplate.getRedisTemplate());
  }

  @Override
  @SuppressWarnings("unchecked")
  public Map<String, List<Object>> readFromLists(List<String> keys) {
    Map<String, List<Object>> out = new HashMap<>();
    List<Object> redisOut =
        RedisUtils.executePipeLine(
            redisTemplate.getRedisTemplate(),
            ((connection, keySerializer, valueSerializer) -> {
              for (String key : keys) {
                connection.lRange(Objects.requireNonNull(keySerializer.serialize(key)), 0, -1);
              }
            }));
    for (int i = 0; i < keys.size(); i++) {
      List<Object> values = (List<Object>) redisOut.get(i);
      if (!CollectionUtils.isEmpty(values)) {
        out.put(keys.get(i), values);
      }
    }
    return out;
  }

  @Override
  public List<Object> readFromList(String key) {
    return readFromLists(Collections.singletonList(key)).getOrDefault(key, Collections.emptyList());
  }

  @Override
  public void appendToListWithListExpiry(String listName, String data, Duration duration) {
    RedisUtils.executePipeLine(
        redisTemplate.getRedisTemplate(),
        (connection, keySerializer, valueSerializer) -> {
          byte[] key = keySerializer.serialize(listName);
          byte[] value = valueSerializer.serialize(data);
          connection.rPush(key, value);
          connection.expire(key, duration.getSeconds());
        });
  }

  @Override
  public void appendToSet(String setName, String... data) {
    redisTemplate.addToSet(setName, data);
  }

  @Override
  public List<String> readFromSet(String setName) {
    Set<String> data = redisTemplate.getMembers(setName);
    if (CollectionUtils.isEmpty(data)) {
      return Collections.emptyList();
    }
    return new ArrayList<>(data);
  }

  @Override
  public Boolean delete(String key) {
    return redisTemplate.delete(key);
  }

  @Override
  public void set(String key, Object data) {
    redisTemplate.set(key, (String) data);
  }

  @Override
  public Object get(String key) {
    return redisTemplate.get(key);
  }

  @Override
  public Object delete(Collection<String> keys) {
    // potential cross slot error
    return deleteAndSet(keys, null);
  }

  @Override
  public Object deleteAndSet(
      Collection<String> keysToBeRemoved, Map<String, Object> objectsToBeStored) {
    return RedisUtils.executePipeLine(
        redisTemplate.getRedisTemplate(),
        ((connection, keySerializer, valueSerializer) -> {
          // potential cross slot error
          for (String key : keysToBeRemoved) {
            connection.del(keySerializer.serialize(key));
          }
          if (!CollectionUtils.isEmpty(objectsToBeStored)) {
            for (Entry<String, Object> entry : objectsToBeStored.entrySet()) {
              connection.set(
                  Objects.requireNonNull(keySerializer.serialize(entry.getKey())),
                  Objects.requireNonNull(valueSerializer.serialize(entry.getValue())));
            }
          }
        }));
  }

  @Override
  public Boolean setIfAbsent(String key, String value, Duration duration) {
    return redisTemplate.setIfAbsent(key, value, duration);
  }

  @Override
  public Long getListSize(String name) {
    return redisTemplate.getListSize(name);
  }

  @Override
  public Long getSortedSetSize(String name) {
    return redisTemplate.getZsetSize(name);
  }

  @Override
  public DataType type(String key) {
    return redisTemplate.type(key);
  }

  @Override
  public Boolean deleteIfSame(String key, String value) {
    return scriptExecutor.execute(delIfSameScript, Collections.singletonList(key), value);
  }

  @Override
  public void addToOrderedSetWithScore(String key, String value, long score) {
    redisTemplate.zadd(key, value, score);
  }

  @Override
  public List<TypedTuple<String>> readFromOrderedSetWithScoreBetween(
      String key, long start, long end) {
    Set<TypedTuple<String>> messages = redisTemplate.zrangeWithScore(key, start, end);
    if (CollectionUtils.isEmpty(messages)) {
      return Collections.emptyList();
    }
    return new ArrayList<>(messages);
  }

  @Override
  public void deleteAll(String key, long min, long max) {
    redisTemplate.zremRangeByScore(key, min, max);
  }
}
