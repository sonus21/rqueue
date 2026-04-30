/*
 * Copyright (c) 2024-2026 Sonu Kumar
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * You may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 */

package com.github.sonus21.rqueue.dao.impl;

import com.github.sonus21.rqueue.config.NatsBackendCondition;
import com.github.sonus21.rqueue.dao.RqueueStringDao;
import java.time.Duration;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.springframework.context.annotation.Conditional;
import org.springframework.data.redis.connection.DataType;
import org.springframework.data.redis.core.ZSetOperations.TypedTuple;
import org.springframework.stereotype.Component;

/**
 * NATS-backend stub {@link RqueueStringDao} for non-Redis backends. Reads return empty/zero/null so
 * dashboard gauges and metric collectors register zero values cleanly; writes are silently
 * ignored. The runtime produce-and-consume path on NATS does not invoke this DAO.
 */
@Component
@Conditional(NatsBackendCondition.class)
public class NatsRqueueStringDao implements RqueueStringDao {

  @Override
  public Map<String, List<Object>> readFromLists(List<String> keys) {
    return Collections.emptyMap();
  }

  @Override
  public List<Object> readFromList(String key) {
    return Collections.emptyList();
  }

  @Override
  public void appendToListWithListExpiry(String listName, String data, Duration duration) {}

  @Override
  public void appendToSet(String setName, String... data) {}

  @Override
  public List<String> readFromSet(String setName) {
    return Collections.emptyList();
  }

  @Override
  public Boolean delete(String key) {
    return Boolean.FALSE;
  }

  @Override
  public void set(String key, Object data) {}

  @Override
  public Object get(String key) {
    return null;
  }

  @Override
  public Object delete(Collection<String> keys) {
    return null;
  }

  @Override
  public Object deleteAndSet(
      Collection<String> keysToBeRemoved, Map<String, Object> objectsToBeStored) {
    return null;
  }

  @Override
  public Boolean setIfAbsent(String key, String value, Duration duration) {
    return Boolean.TRUE;
  }

  @Override
  public Long getListSize(String name) {
    return 0L;
  }

  @Override
  public Long getSortedSetSize(String name) {
    return 0L;
  }

  @Override
  public DataType type(String key) {
    return DataType.NONE;
  }

  @Override
  public Boolean deleteIfSame(String key, String value) {
    return Boolean.FALSE;
  }

  @Override
  public void addToOrderedSetWithScore(String key, String value, long score) {}

  @Override
  public List<TypedTuple<String>> readFromOrderedSetWithScoreBetween(
      String key, long start, long end) {
    return Collections.emptyList();
  }

  @Override
  public void deleteAll(String key, long min, long max) {}
}
