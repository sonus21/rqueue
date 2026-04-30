/*
 * Copyright (c) 2024-2026 Sonu Kumar
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
package com.github.sonus21.rqueue.spring;

/**
 * Backend selector for {@link EnableRqueue}. {@link #AUTO} is the default — Rqueue picks NATS when
 * the jnats client is on the classpath and {@code rqueue.backend=nats}, otherwise Redis.
 */
public enum Backend {
  /** Pick the backend automatically based on classpath and {@code rqueue.backend} property. */
  AUTO,
  /** Force the Redis backend regardless of property/classpath. */
  REDIS,
  /** Force the NATS / JetStream backend (requires jnats on the classpath). */
  NATS
}
