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

package com.github.sonus21.rqueue.core.context;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.github.sonus21.TestBase;
import com.github.sonus21.rqueue.CoreUnitTest;
import org.junit.jupiter.api.Test;

@CoreUnitTest
class DefaultContextTest extends TestBase {

  @Test
  void withValueWithEmptyParentContext() {
    Context context = DefaultContext.withValue(DefaultContext.EMPTY, "test", "test");
    assertEquals("test", context.getValue("test"));
    assertNull(context.getValue("testX"));
  }

  @Test
  void withValueWithNullParentContext() {
    Context context = DefaultContext.withValue(null, "test", "test");
    assertEquals("test", context.getValue("test"));
    assertNull(context.getValue("testX"));
  }

  @Test
  void withValueWithNullKey() {
    assertThrows(
        IllegalArgumentException.class, () -> DefaultContext.withValue(null, null, "test"));
  }

  @Test
  void withValueOverrideValueInChildContext() {
    Context context = DefaultContext.withValue(DefaultContext.EMPTY, "test", "test");
    Context newContext = DefaultContext.withValue(context, "test", "foo");
    assertEquals("foo", newContext.getValue("test"));
    assertNull(newContext.getValue("testX"));
  }

  @Test
  void withValueNewKeyValueInChildContext() {
    Context context = DefaultContext.withValue(DefaultContext.EMPTY, "test", "test");
    Context newContext = DefaultContext.withValue(context, "test2", "foo");
    assertEquals("foo", newContext.getValue("test2"));
  }

  @Test
  void getValueWithNullKey() {
    Context context = DefaultContext.withValue(DefaultContext.EMPTY, "test", 12345);
    assertThrows(IllegalArgumentException.class, () -> context.getValue(null));
  }
}
