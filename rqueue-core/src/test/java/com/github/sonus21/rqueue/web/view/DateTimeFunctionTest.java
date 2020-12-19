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

package com.github.sonus21.rqueue.web.view;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;

import com.github.sonus21.TestBase;
import com.github.sonus21.rqueue.CoreUnitTest;
import java.util.Collections;
import org.jtwig.functions.FunctionRequest;
import org.junit.jupiter.api.Test;

@CoreUnitTest
class DateTimeFunctionTest extends TestBase {
  private final DateTimeFunction function = new DateTimeFunction();

  @Test
  void name() {
    assertEquals("time", function.name());
  }

  @Test
  void execute() {
    FunctionRequest request = mock(FunctionRequest.class);
    doReturn(Collections.singletonList(1588335692988L)).when(request).getArguments();
    assertEquals("01 May 2020 12:21", function.execute(request));
  }
}
