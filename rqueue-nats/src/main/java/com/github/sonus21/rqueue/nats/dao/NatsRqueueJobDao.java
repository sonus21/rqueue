/*
 * Copyright (c) 2024-2026 Sonu Kumar
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * You may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 */

package com.github.sonus21.rqueue.nats.dao;

import com.github.sonus21.rqueue.config.NatsBackendCondition;
import com.github.sonus21.rqueue.dao.RqueueJobDao;
import com.github.sonus21.rqueue.models.db.RqueueJob;
import java.time.Duration;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import org.springframework.context.annotation.Conditional;
import org.springframework.stereotype.Repository;

/**
 * NATS-backend stub for {@link RqueueJobDao}. Job tracking persistence is a Redis-only feature
 * in v1; this stub returns empty/null and silently drops writes so the bean graph stays
 * consistent. A NATS-native implementation can replace this in a follow-up.
 */
@Repository
@Conditional(NatsBackendCondition.class)
public class NatsRqueueJobDao implements RqueueJobDao {
  @Override
  public void createJob(RqueueJob rqueueJob, Duration expiry) {}

  @Override
  public void save(RqueueJob rqueueJob, Duration expiry) {}

  @Override
  public RqueueJob findById(String jobId) {
    return null;
  }

  @Override
  public List<RqueueJob> findJobsByIdIn(Collection<String> jobIds) {
    return Collections.emptyList();
  }

  @Override
  public List<RqueueJob> finByMessageIdIn(List<String> messageIds) {
    return Collections.emptyList();
  }

  @Override
  public List<RqueueJob> finByMessageId(String messageId) {
    return Collections.emptyList();
  }

  @Override
  public void delete(String jobId) {}
}
