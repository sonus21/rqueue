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

package com.github.sonus21.rqueue.models.registry;

import com.github.sonus21.rqueue.models.SerializableBase;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.ToString;

@Getter
@Setter
@Builder
@NoArgsConstructor
@AllArgsConstructor
@EqualsAndHashCode(callSuper = true)
@ToString(callSuper = true)
public class RqueueWorkerPollerView extends SerializableBase {

  private static final long serialVersionUID = 7432122245407288494L;

  private String queue;
  private String workerId;
  private String consumerName;
  private String host;
  private String pid;
  private String status;
  private long lastPollAt;
  private String lastPollAge;
  private Long lastMessageAt;
  private String lastMessageAge;
  private Long lastCapacityExhaustedAt;
  private String lastCapacityExhaustedAge;
  private long capacityExhaustedCount;
}
