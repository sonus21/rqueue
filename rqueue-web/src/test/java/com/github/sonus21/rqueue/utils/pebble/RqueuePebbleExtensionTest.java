/*
 * Copyright (c) 2026 Sonu Kumar
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

package com.github.sonus21.rqueue.utils.pebble;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.github.sonus21.TestBase;
import com.github.sonus21.rqueue.CoreUnitTest;
import io.pebbletemplates.pebble.extension.Function;
import java.util.Collections;
import java.util.Map;
import java.util.TimeZone;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

@CoreUnitTest
class RqueuePebbleExtensionTest extends TestBase {

  @BeforeEach
  void init() {
    TimeZone.setDefault(TimeZone.getTimeZone("UTC"));
  }

  @Test
  void registersReadableAndDurationFunctions() {
    Map<String, Function> functions = new RqueuePebbleExtension().getFunctions();

    assertTrue(functions.containsKey(DateTimeFunction.FUNCTION_NAME));
    assertTrue(functions.containsKey(ReadableDateTimeFunction.FUNCTION_NAME));
    assertTrue(functions.containsKey(DurationFunction.FUNCTION_NAME));
  }

  @Test
  void readableTimeFunctionFormatsReadableTimestamp() {
    ReadableDateTimeFunction function = new ReadableDateTimeFunction();

    Object value = function.execute(Collections.singletonMap("milli", 10000000000L), null, null, 0);

    assertEquals("26 Apr, 1970 at 05:46 PM", value);
  }

  @Test
  void durationFunctionFormatsCompactDuration() {
    DurationFunction function = new DurationFunction();

    Object value = function.execute(Collections.singletonMap("milli", 900000L), null, null, 0);

    assertEquals("15 min", value);
  }
}
