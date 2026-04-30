/*
 * Copyright (c) 2024-2026 Sonu Kumar
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
 * Spring {@link Condition} that matches when the active rqueue backend is {@link Backend#REDIS}.
 * Reads the {@code rqueue.backend} property; absent or unparseable values default to REDIS so the
 * existing behavior is preserved.
 */
public class RedisBackendCondition implements Condition {
  @Override
  public boolean matches(ConditionContext ctx, AnnotatedTypeMetadata md) {
    return resolveBackend(ctx) == Backend.REDIS;
  }

  static Backend resolveBackend(ConditionContext ctx) {
    String raw = ctx.getEnvironment().getProperty("rqueue.backend");
    if (raw == null || raw.isBlank()) {
      return Backend.REDIS;
    }
    try {
      return Backend.valueOf(raw.trim().toUpperCase());
    } catch (IllegalArgumentException ex) {
      return Backend.REDIS;
    }
  }
}
