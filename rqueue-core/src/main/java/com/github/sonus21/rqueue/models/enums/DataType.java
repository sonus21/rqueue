/*
 * Copyright (c) 2020-2023 Sonu Kumar
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
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Getter;

@Getter
@AllArgsConstructor(access = AccessLevel.PRIVATE)
public enum DataType {
  NONE("Select Data Type", false),
  LIST("List", true),
  ZSET("Priority Queue", true),
  KEY("Simple key/value", false),
  SET("Set", false);

  private final String description;
  private final boolean enabled;

  public static DataType convertDataType(org.springframework.data.redis.connection.DataType type) {
    switch (type) {
      case LIST:
        return DataType.LIST;
      case ZSET:
        return DataType.ZSET;
      case SET:
        return DataType.SET;
      case STRING:
        return DataType.KEY;
      default:
        return DataType.NONE;
    }
  }

  public static List<DataType> getEnabledDataTypes() {
    return Arrays.stream(values()).filter(DataType::isEnabled).collect(Collectors.toList());
  }

  public static boolean isUnknown(DataType type) {
    return type == null || type == NONE;
  }
}
