/*
 * Copyright (c) 2024-2026 Sonu Kumar
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * You may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 */

package com.github.sonus21.rqueue.common.impl;

import com.github.sonus21.rqueue.common.RqueueLockManager;
import com.github.sonus21.rqueue.config.NatsBackendCondition;
import java.time.Duration;
import org.springframework.context.annotation.Conditional;
import org.springframework.stereotype.Component;

/**
 * NATS-backend stub {@link RqueueLockManager} for non-Redis backends. Redis-only callers (admin endpoints,
 * the scheduler) are themselves gated; on the NATS path the lock primitive isn't needed because
 * JetStream's durable consumers serialize delivery. This impl returns {@code true} for
 * acquire/release so any inadvertent caller proceeds without blocking; if a caller really
 * depends on mutual exclusion through this manager it will need a backend-specific path.
 */
@Component
@Conditional(NatsBackendCondition.class)
public class NatsRqueueLockManager implements RqueueLockManager {
  @Override
  public boolean acquireLock(String lockKey, String lockValue, Duration duration) {
    return true;
  }

  @Override
  public boolean releaseLock(String lockKey, String lockValue) {
    return true;
  }
}
