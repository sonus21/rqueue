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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

public class StringUtilsTest {

  @Test
  public void isEmpty() {
    assertTrue(StringUtils.isEmpty(null));
    assertTrue(StringUtils.isEmpty(""));
    assertFalse(StringUtils.isEmpty("hello"));
  }

  @Test
  public void clean() {
    assertEquals("", StringUtils.clean("   "));
    assertEquals("test", StringUtils.clean("test"));
    assertEquals("test", StringUtils.clean("    test    "));
    assertEquals("test queue", StringUtils.clean("    test queue   "));
  }

  @Test(expected = IllegalArgumentException.class)
  public void convertToCamelCaseEmpty() {
    StringUtils.convertToCamelCase("   ");
  }

  @Test(expected = IllegalArgumentException.class)
  public void convertToCamelCaseNull() {
    StringUtils.convertToCamelCase(null);
  }

  @Test
  public void convertToCamelCase() {
    assertEquals("url", StringUtils.convertToCamelCase("URL"));
    assertEquals("jobQueue", StringUtils.convertToCamelCase("job-Queue"));
    assertEquals("jobQueue", StringUtils.convertToCamelCase("Job-Queue"));
    assertEquals("jobQueue", StringUtils.convertToCamelCase("JOB-Queue"));
    assertEquals("jobQueue", StringUtils.convertToCamelCase("  JOB-Queue  "));
    assertEquals("jobQueue", StringUtils.convertToCamelCase("  JOB-1233-Queue  "));
    assertEquals("jobQuEue", StringUtils.convertToCamelCase("  JOB-1233-Qu1234eue  "));
    assertEquals("jobQuEueQueue", StringUtils.convertToCamelCase("  JOB-1233-Qu1234eueQUEUE  "));
    assertEquals("1234-1234", StringUtils.convertToCamelCase("  1234-1234  "));
    assertEquals("jB", StringUtils.convertToCamelCase("  jB  "));
    assertEquals("jOb", StringUtils.convertToCamelCase("  jOB  "));
  }

  @Test
  public void groupName() {
    assertEquals("Url", StringUtils.groupName("URL"));
    assertEquals("JobQueue", StringUtils.groupName("job-Queue"));
    assertEquals("JobQueue", StringUtils.groupName("Job-Queue"));
    assertEquals("JobQueue", StringUtils.groupName("JOB-Queue"));
    assertEquals("JobQueue", StringUtils.groupName("  JOB-Queue  "));
    assertEquals("JobQueue", StringUtils.groupName("  JOB-1233-Queue  "));
    assertEquals("JobQuEue", StringUtils.groupName("  JOB-1233-Qu1234eue  "));
    assertEquals("JobQuEueQueue", StringUtils.groupName("  JOB-1233-Qu1234eueQUEUE  "));
    assertEquals("1234-1234", StringUtils.groupName("  1234-1234  "));
    assertEquals("JoB", StringUtils.groupName("  joB  "));
    assertEquals("JB", StringUtils.groupName("  jB  "));
  }
}
