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

package com.github.sonus21.rqueue.utils.backoff;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.github.sonus21.TestBase;
import com.github.sonus21.rqueue.CoreUnitTest;
import org.junit.jupiter.api.Test;

@CoreUnitTest
class FixedTaskExecutionBackOffTest extends TestBase {

  @Test
  void construct() {
    FixedTaskExecutionBackOff backOff = new FixedTaskExecutionBackOff(1000, 100);
    assertEquals(1000L, backOff.getInterval());
    assertEquals(100, backOff.getMaxRetries());
    assertEquals(1000L, backOff.nextBackOff(null, null, 1));
    assertEquals(-1, backOff.nextBackOff(null, null, 101));
  }

  @Test
  void setInterval() {
    FixedTaskExecutionBackOff backOff = new FixedTaskExecutionBackOff();
    backOff.setInterval(200L);
    assertEquals(200L, backOff.nextBackOff(null, null, 1));
    assertEquals(200L, backOff.getInterval());
  }

  @Test
  void setMaxRetries() {
    FixedTaskExecutionBackOff backOff = new FixedTaskExecutionBackOff();
    backOff.setMaxRetries(3);
    assertEquals(5000L, backOff.nextBackOff(null, null, 1));
    assertEquals(3, backOff.getMaxRetries());
    assertEquals(-1, backOff.nextBackOff(null, null, 3));
  }

  @Test
  void constructNegativeInterval() {
    assertThrows(IllegalArgumentException.class, () -> new FixedTaskExecutionBackOff(-1, 100));
  }

  @Test
  void constructNegativeRetries() {
    assertThrows(IllegalArgumentException.class, () -> new FixedTaskExecutionBackOff(100L, -1));
  }
}
