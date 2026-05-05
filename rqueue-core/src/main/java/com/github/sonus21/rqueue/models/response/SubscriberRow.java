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

package com.github.sonus21.rqueue.models.response;

import com.github.sonus21.rqueue.models.SerializableBase;
import com.github.sonus21.rqueue.models.enums.DataType;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.ToString;

/**
 * Per-subscriber row rendered by the queue-detail "Subscribers" section. Joins
 * broker-supplied per-consumer counts with worker-registry status info so the dashboard
 * can drop the standalone "Queue Pollers" table.
 */
@Getter
@Setter
@AllArgsConstructor
@NoArgsConstructor
@Builder
@ToString(callSuper = true)
@EqualsAndHashCode(callSuper = false)
public class SubscriberRow extends SerializableBase {

  private static final long serialVersionUID = 1L;

  /** Logical handler name (resolved consumer name on NATS, listener bean#method on Redis). */
  private String consumerName;

  /** Backend-aware label, e.g. "Queue (Stream)" / "Stream consumer" / "List". */
  private String typeLabel;

  /** The underlying storage handle exposed to the message-explorer modal. */
  private String storageName;

  /** Redis-shaped data type used for the explorer modal's pagination logic. */
  private DataType dataType;

  /**
   * Messages this subscriber is responsible for processing. For brokers with shared pools
   * (Redis, NATS WorkQueue) this is the queue-wide pending count; for NATS Limits streams
   * it's the per-consumer {@code numPending}.
   */
  private long pending;

  /**
   * Indicates {@code pending} is a queue-wide aggregate, not this consumer's exclusive
   * backlog. Templates render a "(shared)" hint when this is true.
   */
  private boolean pendingShared;

  /** Messages delivered but not yet acknowledged by this subscriber. */
  private long inFlight;

  /** Worker status: ACTIVE / STALE / UNKNOWN. */
  private String status;

  /** Host running the active worker, when known. */
  private String host;

  /** PID of the active worker, when known (kept as String to mirror the worker registry). */
  private String pid;

  /** Epoch ms of the most recent poll observed for this subscriber. */
  private long lastPollAt;

  /** Human-formatted "last poll age" — populated server-side for Pebble. */
  private String lastPollAge;
}
