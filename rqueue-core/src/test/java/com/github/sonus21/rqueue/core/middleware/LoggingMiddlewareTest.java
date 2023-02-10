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

package com.github.sonus21.rqueue.core.middleware;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.doReturn;

import com.github.sonus21.TestBase;
import com.github.sonus21.rqueue.CoreUnitTest;
import com.github.sonus21.rqueue.core.Job;
import com.github.sonus21.rqueue.utils.RqueueMessageTestUtils;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

@CoreUnitTest
class LoggingMiddlewareTest extends TestBase {

  @Mock
  private Job job;

  @BeforeEach
  public void init() {
    MockitoAnnotations.openMocks(this);
  }

  @Test
  void handle() throws Exception {
    LoggingMiddleware loggingMiddleware = new LoggingMiddleware();
    doReturn(RqueueMessageTestUtils.createMessage("test-queue")).when(job).getRqueueMessage();
    AtomicInteger atomicInteger = new AtomicInteger();
    loggingMiddleware.handle(
        job,
        () -> {
          atomicInteger.incrementAndGet();
          return null;
        });
    assertEquals(1, atomicInteger.get());
  }
}
