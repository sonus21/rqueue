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
import com.github.sonus21.rqueue.dao.RqueueQStatsDao;
import com.github.sonus21.rqueue.models.db.QueueStatistics;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import org.springframework.context.annotation.Conditional;
import org.springframework.stereotype.Repository;

/**
 * NATS-backend stub for {@link RqueueQStatsDao}. The Redis impl persists per-queue daily
 * aggregates as serialized {@link QueueStatistics} objects driving the dashboard charts; on
 * NATS we no-op writes and return empty reads in v1 so that
 * {@code RqueueJobMetricsAggregatorService} can boot without a missing-bean failure even
 * though the chart panel is empty.
 *
 * <p>Replace with a NATS-native impl (a dedicated {@code rqueue-queue-stats} KV bucket
 * mirroring the pattern of {@code NatsRqueueSystemConfigDao}) when chart support lands for
 * NATS.
 */
@Repository
@Conditional(NatsBackendCondition.class)
public class NatsRqueueQStatsDao implements RqueueQStatsDao {

  @Override
  public QueueStatistics findById(String id) {
    return null;
  }

  @Override
  public List<QueueStatistics> findAll(Collection<String> ids) {
    return Collections.emptyList();
  }

  @Override
  public void save(QueueStatistics queueStatistics) {
    // intentionally no-op until a NATS-native chart store lands
  }
}
