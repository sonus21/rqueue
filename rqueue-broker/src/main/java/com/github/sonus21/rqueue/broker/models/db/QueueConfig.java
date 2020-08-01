/*
 * Copyright 2020 Sonu Kumar
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.github.sonus21.rqueue.broker.models.db;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.github.sonus21.rqueue.models.SerializableBase;
import java.util.Map;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.ToString;
import org.springframework.util.CollectionUtils;

@Getter
@Setter
@NoArgsConstructor
@ToString
public class QueueConfig extends SerializableBase {
  private static final long serialVersionUID = -7377265707903362101L;
  private String id;
  private String name;
  private String simpleQueue;
  private String delayedQueue;
  private String processingQueue;
  private long visibilityTimeout;
  private Map<String, Integer> priority;
  private Map<String, Integer> systemPriority;
  private boolean systemQueue;
  private long createdOn;
  private long updatedOn;

  @JsonIgnore
  public boolean isValidPriority(String priority) {
    if (CollectionUtils.isEmpty(this.priority)) {
      return false;
    }
    return this.priority.containsKey(priority);
  }
}
