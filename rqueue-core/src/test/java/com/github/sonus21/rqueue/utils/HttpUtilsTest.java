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

package com.github.sonus21.rqueue.utils;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.github.sonus21.TestBase;
import com.github.sonus21.rqueue.CoreUnitTest;
import org.junit.jupiter.api.Test;

@CoreUnitTest
class HttpUtilsTest extends TestBase {

  @Test
  void joinPath() {
    assertEquals("/", HttpUtils.joinPath(null, null));
    assertEquals("/", HttpUtils.joinPath(null, "/"));
    assertEquals("/foo/", HttpUtils.joinPath(null, "/foo"));
    assertEquals("/foo/bar/", HttpUtils.joinPath(null, "/foo/", "/bar"));
    assertEquals("/foo/bar/", HttpUtils.joinPath("/foo/", null, "/bar"));
    assertEquals("/foo/bar/", HttpUtils.joinPath("/foo/", "/", "/bar"));
    assertEquals("/foo/bar/", HttpUtils.joinPath("foo", "/", "bar"));
  }
}
