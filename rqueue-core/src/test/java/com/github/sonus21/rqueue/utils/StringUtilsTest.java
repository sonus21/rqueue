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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.github.sonus21.TestBase;
import com.github.sonus21.rqueue.CoreUnitTest;
import org.junit.jupiter.api.Test;

@CoreUnitTest
class StringUtilsTest extends TestBase {

  @Test
  void isEmpty() {
    assertTrue(StringUtils.isEmpty(null));
    assertTrue(StringUtils.isEmpty(""));
    assertFalse(StringUtils.isEmpty("hello"));
  }

  @Test
  void clean() {
    assertEquals("", StringUtils.clean("   "));
    assertNull(StringUtils.clean(null));
    assertEquals("test", StringUtils.clean("test"));
    assertEquals("test", StringUtils.clean("    test    "));
    assertEquals("test queue", StringUtils.clean("    test queue   "));
  }

  @Test
  void convertToCamelCaseEmpty() {
    assertThrows(IllegalArgumentException.class, () -> StringUtils.convertToCamelCase("   "));
  }

  @Test
  void convertToCamelCaseNull() {
    assertThrows(IllegalArgumentException.class, () -> StringUtils.convertToCamelCase(null));
  }

  @Test
  void getBeanName() {
    assertEquals("url", StringUtils.getBeanName("URL"));
    assertEquals("jobQueue", StringUtils.getBeanName("job-Queue"));
    assertEquals("jobQueue", StringUtils.getBeanName("Job-Queue"));
    assertEquals("jobQueue", StringUtils.getBeanName("JOB-Queue"));
    assertEquals("jobQueue", StringUtils.getBeanName("  JOB-Queue  "));
    assertEquals("jobQueue1233", StringUtils.getBeanName("  JOB-Queue1233-  "));
    assertEquals("job1233Qu1234Eue", StringUtils.getBeanName("  JOB-1233-Qu1234eue  "));
    assertEquals("job1233Qu1234EueQueue", StringUtils.getBeanName("  JOB-1233-Qu1234eueQUEUE  "));
    assertEquals("bean12341234", StringUtils.getBeanName("  1234-1234  "));
    assertEquals("jB", StringUtils.getBeanName("  jB  "));
    assertEquals("jOb", StringUtils.getBeanName("  jOB  "));
  }

  @Test
  void convertToCamelCase() {
    assertEquals("url", StringUtils.convertToCamelCase("URL"));
    assertEquals("jobQueue", StringUtils.convertToCamelCase("job-Queue"));
    assertEquals("jobQueue", StringUtils.convertToCamelCase("Job-Queue"));
    assertEquals("jobQueue", StringUtils.convertToCamelCase("JOB-Queue"));
    assertEquals("jobQueue", StringUtils.convertToCamelCase("  JOB-Queue  "));
    assertEquals("jobQueue1233", StringUtils.convertToCamelCase("  JOB-Queue1233-  "));
    assertEquals("job1233Qu1234Eue", StringUtils.convertToCamelCase("  JOB-1233-Qu1234eue  "));
    assertEquals(
        "job1233Qu1234EueQueue", StringUtils.convertToCamelCase("  JOB-1233-Qu1234eueQUEUE  "));
    assertEquals("", StringUtils.convertToCamelCase("  1234-1234  "));
    assertEquals("jB", StringUtils.convertToCamelCase("  jB  "));
    assertEquals("jOb", StringUtils.convertToCamelCase("  jOB  "));
  }

  @Test
  void groupName() {
    assertEquals("Url", StringUtils.groupName("URL"));
    assertEquals("JobQueue", StringUtils.groupName("job-Queue"));
    assertEquals("JobQueue", StringUtils.groupName("Job-Queue"));
    assertEquals("JobQueue", StringUtils.groupName("JOB-Queue"));
    assertEquals("JobQueue", StringUtils.groupName("  JOB-Queue  "));
    assertEquals("Job1233Queue", StringUtils.groupName("  JOB-1233-Queue  "));
    assertEquals("Job1233Qu1234Eue", StringUtils.groupName("  JOB-1233-Qu1234eue  "));
    assertEquals("Job1233Qu1234EueQueue", StringUtils.groupName("  JOB-1233-Qu1234eueQUEUE  "));
    assertEquals("Group12341234", StringUtils.groupName("  1234-1234  "));
    assertEquals("JoB", StringUtils.groupName("  joB  "));
    assertEquals("JB", StringUtils.groupName("  jB  "));
  }
}
