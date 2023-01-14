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
class ExponentialTaskExecutionBackOffTest extends TestBase {

  @Test
  void negativeInitialValue() {
    assertThrows(
        IllegalArgumentException.class,
        () -> new ExponentialTaskExecutionBackOff(-1, 100, 1.5, 100));
  }

  @Test
  void smallerMaxInterval() {
    assertThrows(
        IllegalArgumentException.class,
        () -> new ExponentialTaskExecutionBackOff(100, 99, 1.5, 100));
  }

  @Test
  void smallerMultiplier() {
    assertThrows(
        IllegalArgumentException.class,
        () -> new ExponentialTaskExecutionBackOff(1000L, 3000000L, 0.5, 100));
  }

  @Test
  void negativeMaxRetries() {
    assertThrows(
        IllegalArgumentException.class,
        () -> new ExponentialTaskExecutionBackOff(1000L, 3000000L, 1.5, -1));
  }

  @Test
  void construct() {
    ExponentialTaskExecutionBackOff e =
        new ExponentialTaskExecutionBackOff(1000L, 3000000L, 1.5, 3);
    assertEquals((long) (Math.pow(1.5, 3) * 1000L), e.getDelay(3));
  }

  @Test
  void setMaxRetries() {
    ExponentialTaskExecutionBackOff e = new ExponentialTaskExecutionBackOff();
    e.setMaxRetries(3);
    assertEquals(-1, e.nextBackOff(null, null, 3));
    assertEquals(2250, e.nextBackOff(null, null, 1));
    assertEquals(3, e.getMaxRetries());
  }

  @Test
  void setMultiplier() {
    ExponentialTaskExecutionBackOff e = new ExponentialTaskExecutionBackOff();
    e.setMultiplier(1.2);
    assertEquals((long) (Math.pow(1.2, 3) * 1500), e.nextBackOff(null, null, 3));
    assertEquals(1.2, e.getMultiplier(), 0.0001);
  }

  @Test
  void setMaxInterval() {
    ExponentialTaskExecutionBackOff e = new ExponentialTaskExecutionBackOff();
    e.setMaxInterval(100000L);
    assertEquals(100000L, e.nextBackOff(null, null, 60));
    assertEquals(100000L, e.getMaxInterval());
  }

  @Test
  void setMaxLongValue() {
    ExponentialTaskExecutionBackOff e = new ExponentialTaskExecutionBackOff();
    assertEquals(-1, e.nextBackOff(null, null, 110));
  }

  @Test
  void setInitialInterval() {
    ExponentialTaskExecutionBackOff e = new ExponentialTaskExecutionBackOff();
    e.setInitialInterval(2000L);
    assertEquals((long) (Math.pow(1.5, 3) * 2000), e.nextBackOff(null, null, 3));
    assertEquals(2000L, e.getInitialInterval());
  }
}
