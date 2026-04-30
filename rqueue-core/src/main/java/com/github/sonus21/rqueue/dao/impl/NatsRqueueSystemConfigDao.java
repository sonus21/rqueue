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
import com.github.sonus21.rqueue.dao.RqueueSystemConfigDao;
import com.github.sonus21.rqueue.models.db.QueueConfig;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import org.springframework.context.annotation.Conditional;
import org.springframework.stereotype.Repository;

/** NATS-backend stub {@link RqueueSystemConfigDao} for non-Redis backends. */
@Repository
@Conditional(NatsBackendCondition.class)
public class NatsRqueueSystemConfigDao implements RqueueSystemConfigDao {
  @Override
  public QueueConfig getConfigByName(String name) {
    return null;
  }

  @Override
  public List<QueueConfig> getConfigByNames(Collection<String> names) {
    return Collections.emptyList();
  }

  @Override
  public QueueConfig getConfigByName(String name, boolean cached) {
    return null;
  }

  @Override
  public QueueConfig getQConfig(String id, boolean cached) {
    return null;
  }

  @Override
  public List<QueueConfig> findAllQConfig(Collection<String> ids) {
    return Collections.emptyList();
  }

  @Override
  public void saveQConfig(QueueConfig queueConfig) {}

  @Override
  public void saveAllQConfig(List<QueueConfig> newConfigs) {}

  @Override
  public void clearCacheByName(String name) {}
}
