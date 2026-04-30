/*
 * Copyright (c) 2019-2026 Sonu Kumar
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * You may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 */

package com.github.sonus21.rqueue.config;

/**
 * The active rqueue backend. Bound from the {@code rqueue.backend} configuration property
 * (Spring binds the string value case-insensitively to the matching enum constant); defaults to
 * {@link #REDIS}.
 *
 * <p>{@link RqueueConfig#getBackend()} exposes the resolved value so any bean that wants to
 * branch on the active backend can check it directly instead of relying on classpath probes for
 * {@code RedisConnectionFactory} or {@code JetStream}.
 */
public enum Backend {
  REDIS,
  NATS
}
