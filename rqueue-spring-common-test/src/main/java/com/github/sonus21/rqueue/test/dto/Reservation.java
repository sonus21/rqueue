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

package com.github.sonus21.rqueue.test.dto;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.ToString;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.RandomUtils;

@AllArgsConstructor
@NoArgsConstructor
@Getter
@Setter
@ToString
@EqualsAndHashCode(callSuper = true)
public class Reservation extends BaseQueueMessage {

  private int requestId;
  private Map<String, String> otherDetails;

  public static Reservation newInstance() {
    Map<String, String> other = new HashMap<>();
    int count = RandomUtils.nextInt(1, 10);
    for (int i = 0; i < count; i++) {
      String key = RandomStringUtils.randomAlphabetic(10);
      String value = RandomStringUtils.randomAlphabetic(10);
      other.put(key, value);
    }
    Reservation reservation = new Reservation(RandomUtils.nextInt(), other);
    reservation.setId(UUID.randomUUID().toString());
    return reservation;
  }
}
