/*
 *  Copyright 2021 Sonu Kumar
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *         https://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and limitations under the License.
 *
 */

package com.github.sonus21.rqueue.web.view;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.github.sonus21.TestBase;
import com.github.sonus21.rqueue.CoreUnitTest;
import com.github.sonus21.rqueue.utils.pebble.DateTimeFunction;
import java.util.Collections;
import org.junit.jupiter.api.Test;

@CoreUnitTest
class DateTimeFunctionTest extends TestBase {

  private final DateTimeFunction function = new DateTimeFunction();

  @Test
  void getArguments() {
    assertEquals(Collections.singletonList("milli"), function.getArgumentNames());
  }

  @Test
  void execute() {
    assertEquals(
        "2020-05-01 12:21",
        function.execute(Collections.singletonMap("milli", 1588335692988L), null, null, -1));
  }
}
