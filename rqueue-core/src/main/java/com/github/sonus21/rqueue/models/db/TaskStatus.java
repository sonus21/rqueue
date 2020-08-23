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

package com.github.sonus21.rqueue.models.db;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Getter;

@AllArgsConstructor(access = AccessLevel.PRIVATE)
@Getter
public enum TaskStatus {
  MOVED_TO_DLQ("Moved to dead letter queue messages", true),
  SUCCESSFUL("Successful execution", true),
  DELETED("Message deleted", false),
  DISCARDED("Message discarded", true),
  RETRIED("Retired at least once", true),
  FAILED("failed", false),
  IGNORED("Ignored task", false),
  QUEUE_INACTIVE("Queue inactive", false),
  PROCESSING("Processing this task", false);

  private String description;
  private boolean chartEnabled;

  public static List<TaskStatus> getActiveChartStatus() {
    return Arrays.stream(values()).filter(TaskStatus::isChartEnabled).collect(Collectors.toList());
  }
}
