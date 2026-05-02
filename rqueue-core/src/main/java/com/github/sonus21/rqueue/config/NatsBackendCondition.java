/*
 * Copyright (c) 2026 Sonu Kumar
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * You may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 */

package com.github.sonus21.rqueue.config;

import org.springframework.context.annotation.Condition;
import org.springframework.context.annotation.ConditionContext;
import org.springframework.core.type.AnnotatedTypeMetadata;

/**
 * Spring {@link Condition} that matches when the active rqueue backend is {@link Backend#NATS}.
 * Companion to {@link RedisBackendCondition}; together they ensure every Redis-shaped interface
 * gets exactly one bean (Redis impl when backend=redis, no-op stub when backend=nats) so
 * consumers don't need {@code @Autowired(required = false)} or null guards.
 */
public class NatsBackendCondition implements Condition {
  @Override
  public boolean matches(ConditionContext ctx, AnnotatedTypeMetadata md) {
    return RedisBackendCondition.resolveBackend(ctx) == Backend.NATS;
  }
}
