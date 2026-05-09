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
import com.github.sonus21.rqueue.models.enums.NavTab;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.ToString;

/**
 * Row in the queue-detail "Terminal Storage" table — one entry per shared bucket
 * (COMPLETED messages, DEAD letter queues). These are not per-consumer; the broker
 * contributes a single shared count.
 */
@Getter
@Setter
@AllArgsConstructor
@NoArgsConstructor
@Builder
@ToString(callSuper = true)
@EqualsAndHashCode(callSuper = false)
public class TerminalStorageRow extends SerializableBase {

  private static final long serialVersionUID = 1L;

  /** Tab category — COMPLETED / DEAD. Used for the "Bucket" column label. */
  private NavTab tab;

  /** Backend-aware label, e.g. "Completed (KV)" / "Dead Letter (Stream)" / "ZSET" / "LIST". */
  private String typeLabel;

  /** Underlying storage handle (e.g. Redis ZSET key, JetStream stream name). */
  private String storageName;

  /** Redis-shaped data type used by the explorer modal's pagination logic. */
  private DataType dataType;

  /**
   * Number of messages currently in this bucket. Negative values render as "Queue-backed"
   * (matches the existing data-detail semantics for DLQs whose consumer is enabled).
   */
  private long size;

  /** {@code true} when {@code size} is a best-effort estimate rather than an exact count. */
  private boolean approximate;
}
