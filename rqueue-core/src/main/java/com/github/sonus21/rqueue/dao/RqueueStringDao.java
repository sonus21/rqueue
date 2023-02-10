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

package com.github.sonus21.rqueue.dao;

import java.time.Duration;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import org.springframework.data.redis.connection.DataType;
import org.springframework.data.redis.core.ZSetOperations.TypedTuple;

public interface RqueueStringDao {

  Map<String, List<Object>> readFromLists(List<String> keys);

  List<Object> readFromList(String key);

  void appendToListWithListExpiry(String listName, String data, Duration duration);

  void appendToSet(String setName, String... data);

  List<String> readFromSet(String setName);

  Boolean delete(String key);

  void set(String key, Object data);

  Object get(String key);

  Object delete(Collection<String> keys);

  Object deleteAndSet(Collection<String> keysToBeRemoved, Map<String, Object> objectsToBeStored);

  Boolean setIfAbsent(String key, String value, Duration duration);

  Long getListSize(String name);

  Long getSortedSetSize(String name);

  DataType type(String key);

  Boolean deleteIfSame(String key, String value);

  void addToOrderedSetWithScore(String key, String value, long score);

  List<TypedTuple<String>> readFromOrderedSetWithScoreBetween(String key, long start, long end);

  void deleteAll(String key, long min, long max);
}
