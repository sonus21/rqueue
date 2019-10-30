/*
 * Copyright (c)  2019-2019, Sonu Kumar
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *       https://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */

package com.github.sonus21.rqueue.spring.boot.integration.app.dto;

import java.util.UUID;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.ToString;
import org.apache.commons.lang3.RandomUtils;

@Getter
@Setter
@ToString(callSuper = true)
@EqualsAndHashCode(callSuper = true)
@NoArgsConstructor
public class Job extends BaseQueueMessage {
  private String type;

  public Job(String id, String type) {
    super(id);
    this.type = type;
  }

  enum Type {
    FULL_TIME,
    PART_TIME,
    CONTRACT
  }

  public static Job newInstance() {
    Type[] types = Type.values();
    int typeId = RandomUtils.nextInt(0, types.length);
    String id = UUID.randomUUID().toString();
    String type = types[typeId].name();
    return new Job(id, type);
  }
}
