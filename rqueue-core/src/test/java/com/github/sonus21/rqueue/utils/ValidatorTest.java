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

package com.github.sonus21.rqueue.utils;

import static org.junit.jupiter.api.Assertions.assertThrows;

import com.github.sonus21.TestBase;
import com.github.sonus21.rqueue.CoreUnitTest;
import org.junit.jupiter.api.Test;

@CoreUnitTest
class ValidatorTest extends TestBase {

  @Test
  void validateMessage() {
    assertThrows(IllegalArgumentException.class, () -> Validator.validateMessage(null));
  }

  @Test
  void validateMessageId() {
    assertThrows(IllegalArgumentException.class, () -> Validator.validateMessageId(null));
  }

  @Test
  void validateRetryCount() {
    assertThrows(IllegalArgumentException.class, () -> Validator.validateRetryCount(-1));
  }

  @Test
  void validateDelay() {
    assertThrows(IllegalArgumentException.class, () -> Validator.validateDelay(-100L));
  }

  @Test
  void validateQueue() {
    assertThrows(IllegalArgumentException.class, () -> Validator.validateQueue(null));
  }

  @Test
  void validatePriority() {
    assertThrows(IllegalArgumentException.class, () -> Validator.validatePriority(null));
  }
}
