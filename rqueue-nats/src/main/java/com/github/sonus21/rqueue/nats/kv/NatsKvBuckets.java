/*
 * Copyright (c) 2026 Sonu Kumar
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

package com.github.sonus21.rqueue.nats.kv;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * Single source of truth for the JetStream KV bucket names the NATS backend depends on. Each
 * store / dao references the constant here instead of hard-coding its own copy, and
 * {@link NatsKvBucketValidator} walks {@link #ALL_BUCKETS} at
 * startup to fail fast when {@code rqueue.nats.autoCreateKvBuckets=false} and any bucket is
 * missing.
 *
 * <p>If you add a new KV-backed store, add its bucket name here.
 */
public final class NatsKvBuckets {

  /** Per-queue {@code QueueConfig} records (registered queues, DLQ wiring, flags). */
  public static final String QUEUE_CONFIG = "rqueue-queue-config";

  /** {@code RqueueJob} execution history per message id. */
  public static final String JOBS = "rqueue-jobs";

  /** Distributed locks (scheduler leadership, message-level locks). */
  public static final String LOCKS = "rqueue-locks";

  /** Per-message metadata (delivery status, retry count, dead-letter flags). */
  public static final String MESSAGE_METADATA = "rqueue-message-metadata";

  /** Worker process info (host, pid, version, last-seen). */
  public static final String WORKERS = "rqueue-workers";

  /** Per-(queue, worker) heartbeats. Keys flattened as {@code <queue>__<worker>}. */
  public static final String WORKER_HEARTBEATS = "rqueue-worker-heartbeats";

  /** Per-queue daily execution statistics (success, discard, DLQ, retry, run-time). */
  public static final String QUEUE_STATS = "rqueue-queue-stats";

  /** All buckets the NATS backend will use, in stable order. */
  public static final List<String> ALL_BUCKETS = Collections.unmodifiableList(Arrays.asList(
      QUEUE_CONFIG, JOBS, LOCKS, MESSAGE_METADATA, WORKERS, WORKER_HEARTBEATS, QUEUE_STATS));

  private NatsKvBuckets() {}
}
