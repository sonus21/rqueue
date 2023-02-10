/*
 * Copyright (c) 2019-2023 Sonu Kumar
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

package com.github.sonus21.rqueue.models.enums;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import lombok.AllArgsConstructor;
import lombok.Getter;

@AllArgsConstructor
@Getter
public enum ChartDataType {
  SUCCESSFUL("Successful execution", true),
  DISCARDED("Message discarded", true),
  MOVED_TO_DLQ("Moved to dead letter queue messages", true),
  RETRIED("Retired at least once", true),
  EXECUTION("Min Execution time", false);
  private final String description;
  private final boolean userView;

  public static List<ChartDataType> getActiveCharts() {
    return Arrays.stream(values()).filter(ChartDataType::isUserView).collect(Collectors.toList());
  }
}
