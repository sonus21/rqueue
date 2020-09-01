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

package com.github.sonus21.rqueue.utils;

import org.junit.Test;

public class ValidatorTest {

  @Test(expected = IllegalArgumentException.class)
  public void validateMessage() {
    Validator.validateMessage(null);
  }

  @Test(expected = IllegalArgumentException.class)
  public void validateMessageId() {
    Validator.validateMessageId(null);
  }

  @Test(expected = IllegalArgumentException.class)
  public void validateRetryCount() {
    Validator.validateRetryCount(-1);
  }

  @Test(expected = IllegalArgumentException.class)
  public void validateDelay() {
    Validator.validateDelay(-100L);
  }

  @Test(expected = IllegalArgumentException.class)
  public void validateQueue() {
    Validator.validateQueue(null);
  }

  @Test(expected = IllegalArgumentException.class)
  public void validatePriority() {
    Validator.validatePriority(null);
  }
}
