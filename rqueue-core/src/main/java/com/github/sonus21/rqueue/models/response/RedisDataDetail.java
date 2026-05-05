/*
 * Copyright (c) 2019-2026 Sonu Kumar
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
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.ToString;

@Getter
@Setter
@AllArgsConstructor
@NoArgsConstructor
@ToString(callSuper = true)
@EqualsAndHashCode(callSuper = false)
public class RedisDataDetail extends SerializableBase {

  private static final long serialVersionUID = 1056616946630817927L;
  private String name;
  private DataType type;
  private long size;

  /**
   * Backend-aware human-readable label for the {@link #type}. The legacy templates surface
   * this directly so deployments can display "Queue" / "Stream" on NATS instead of Redis-shaped
   * "LIST" / "ZSET" tokens. When unset, templates default to {@code type.name()}.
   */
  private String typeLabel;

  /**
   * Indicates that {@link #size} is an approximation rather than an exact count. NATS
   * Limits-retention streams compute pending size from {@code lastSeq - min(delivered.streamSeq)}
   * across durable consumers, which is approximate when filter subjects or per-consumer
   * positions diverge. Templates render the size with a {@code ~} prefix when this is set.
   */
  private boolean approximate;

  /**
   * Optional consumer name when the row represents a single subscriber's view of a shared
   * stream (e.g. NATS Limits-retention streams). Renders next to the stream name in the
   * "Name" column so each durable consumer's lag is visible separately.
   */
  private String consumerName;

  public RedisDataDetail(String name, DataType type, long size) {
    this(name, type, size, null, false, null);
  }
}
