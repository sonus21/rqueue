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

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;

import java.util.Collections;
import org.jtwig.functions.FunctionRequest;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.StrictStubs.class)
public class DateTimeFunctionTest {
  private DateTimeFunction function = new DateTimeFunction();

  @Test
  public void name() {
    assertEquals("time", function.name());
  }

  @Test
  public void execute() {
    FunctionRequest request = mock(FunctionRequest.class);
    doReturn(Collections.singletonList(1588335692988L)).when(request).getArguments();
    assertEquals("01 May 2020 12:21", function.execute(request));
  }
}
