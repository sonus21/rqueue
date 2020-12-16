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

package com.github.sonus21.rqueue.dao.impl;

import com.github.sonus21.rqueue.common.RqueueRedisTemplate;
import com.github.sonus21.rqueue.config.RqueueConfig;
import com.github.sonus21.rqueue.dao.RqueueStringDao;
import com.github.sonus21.rqueue.utils.RedisUtils;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Set;
import org.springframework.data.redis.connection.DataType;
import org.springframework.util.CollectionUtils;

public class RqueueStringDaoImpl implements RqueueStringDao {
  private RqueueRedisTemplate<String> redisTemplate;

  public RqueueStringDaoImpl(RqueueConfig rqueueConfig) {
    redisTemplate = new RqueueRedisTemplate<>(rqueueConfig.getConnectionFactory());
  }

  @Override
  public void appendToList(String listName, String data) {
    redisTemplate.rpush(listName, data);
  }

  @Override
  public void appendToListWithListExpiry(String listName, String data, Duration duration) {
    RedisUtils.executePipeLine(
        redisTemplate.getRedisTemplate(),
        (connection, keySerializer, valueSerializer) -> {
          connection.rPush(
              listName.getBytes(StandardCharsets.UTF_8), data.getBytes(StandardCharsets.UTF_8));
          connection.expire(listName.getBytes(StandardCharsets.UTF_8), duration.getSeconds());
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
    //potential cross slot error
    return deleteAndSet(keys, null);
  }

  @Override
  public Object deleteAndSet(
      Collection<String> keysToBeRemoved, Map<String, Object> objectsToBeStored) {
    return RedisUtils.executePipeLine(
        redisTemplate.getRedisTemplate(),
        ((connection, keySerializer, valueSerializer) -> {
          //potential cross slot error
          for (String key : keysToBeRemoved) {
            connection.del(key.getBytes());
          }
          if (!CollectionUtils.isEmpty(objectsToBeStored)) {
            for (Entry<String, Object> entry : objectsToBeStored.entrySet()) {
              connection.set(
                  entry.getKey().getBytes(),
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
  public void ltrim(String listName, int start, int end) {
    redisTemplate.ltrim(listName, start, end);
  }
}
