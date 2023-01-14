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

package com.github.sonus21.rqueue.listener;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.github.sonus21.TestBase;
import com.github.sonus21.rqueue.CoreUnitTest;
import java.util.Collections;
import org.junit.jupiter.api.Test;

@CoreUnitTest
class MappingInformationTest extends TestBase {

  @Test
  void testToString() {
    MappingInformation mappingInformation =
        MappingInformation.builder()
            .queueNames(Collections.singleton("test-queue"))
            .active(true)
            .build();
    assertEquals("test-queue", mappingInformation.toString());
  }

  @Test
  void isValidNoQueue() {
    MappingInformation mappingInformation =
        MappingInformation.builder().active(true).queueNames(Collections.emptySet()).build();
    assertFalse(mappingInformation.isValid());
  }

  @Test
  void isValidInvalidVisibilityTimeout() {
    MappingInformation mappingInformation =
        MappingInformation.builder()
            .active(true)
            .queueNames(Collections.emptySet())
            .visibilityTimeout(100)
            .build();
    assertFalse(mappingInformation.isValid());
  }

  @Test
  void isValid() {
    MappingInformation mappingInformation =
        MappingInformation.builder()
            .active(true)
            .queueNames(Collections.singleton("test"))
            .visibilityTimeout(1101)
            .build();
    assertTrue(mappingInformation.isValid());
  }

  @Test
  void equality() {
    MappingInformation mappingInformation =
        MappingInformation.builder()
            .active(true)
            .queueNames(Collections.singleton("test"))
            .visibilityTimeout(1101)
            .build();

    MappingInformation mappingInformation1 =
        MappingInformation.builder()
            .active(false)
            .queueNames(Collections.singleton("test"))
            .visibilityTimeout(1201)
            .build();
    assertEquals(mappingInformation, mappingInformation1);
  }

  @Test
  void compare() {
    MappingInformation mappingInformation =
        MappingInformation.builder()
            .active(true)
            .queueNames(Collections.singleton("test"))
            .visibilityTimeout(1101)
            .build();

    MappingInformation mappingInformation1 =
        MappingInformation.builder()
            .active(false)
            .queueNames(Collections.singleton("test"))
            .visibilityTimeout(1201)
            .build();
    assertEquals(0, mappingInformation.compareTo(mappingInformation1));
  }
}
