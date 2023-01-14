/*
 * Copyright (c) 2021-2023 Sonu Kumar
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

package com.github.sonus21;

import java.util.Random;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public abstract class RandomUtils {

  public static Random random;

  static {
    String seed = System.getenv("TEST_SEED");
    long randomSeed = 0;
    if (seed != null) {
      try {
        randomSeed = Long.parseLong(seed);
      } catch (NumberFormatException e) {
        log.error("Invalid number format for log seed {}", seed);
      }
    }
    if (randomSeed == 0) {
      randomSeed = System.currentTimeMillis();
    }
    random = new Random(randomSeed);
    log.error("Test random seed is {}", randomSeed);
  }

  public static long randomTime(long min, long max) {
    return min + random.nextInt((int) (max - min));
  }
}
