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
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;

import com.github.sonus21.TestBase;
import com.github.sonus21.rqueue.CoreUnitTest;
import com.github.sonus21.rqueue.core.Job;
import com.github.sonus21.rqueue.models.enums.JobStatus;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

@CoreUnitTest
class LockMiddlewareTest extends TestBase {

  @Mock
  private Job job;

  @BeforeEach
  public void init() {
    MockitoAnnotations.openMocks(this);
  }

  @Test
  void handleLockIsNotAcquired() throws Exception {
    AtomicInteger atomicInteger = new AtomicInteger();
    AtomicInteger releaseLockCounter = new AtomicInteger();
    LockMiddleware lockMiddleware =
        new LockMiddleware() {
          @Override
          public String acquireLock(Job job) {
            return null;
          }

          @Override
          public void releaseLock(Job job, String lockIdentifier) {
            releaseLockCounter.incrementAndGet();
          }
        };

    lockMiddleware.handle(
        job,
        () -> {
          atomicInteger.incrementAndGet();
          return null;
        });
    verify(job, times(1))
        .release(JobStatus.FAILED, LockMiddleware.REASON, lockMiddleware.releaseIn(job));
    assertEquals(1, releaseLockCounter.get());
    assertEquals(0, atomicInteger.get());
  }

  @Test
  void handleLockIsAcquired() throws Exception {
    AtomicInteger atomicInteger = new AtomicInteger();
    AtomicInteger releaseLockCounter = new AtomicInteger();
    LockMiddleware lockMiddleware =
        new LockMiddleware() {
          @Override
          public String acquireLock(Job job) {
            return "test-lock";
          }

          @Override
          public void releaseLock(Job job, String lockIdentifier) {
            releaseLockCounter.incrementAndGet();
            assertEquals(lockIdentifier, "test-lock");
          }
        };

    lockMiddleware.handle(
        job,
        () -> {
          atomicInteger.incrementAndGet();
          return null;
        });
    verifyNoInteractions(job);
    assertEquals(1, releaseLockCounter.get());
    assertEquals(1, atomicInteger.get());
  }

  @Test
  void handleLockIsNotAcquireReleaseIn() throws Exception {
    AtomicInteger atomicInteger = new AtomicInteger();
    AtomicInteger releaseLockCounter = new AtomicInteger();
    LockMiddleware lockMiddleware =
        new LockMiddleware() {
          @Override
          public String acquireLock(Job job) {
            return null;
          }

          @Override
          public void releaseLock(Job job, String lockIdentifier) {
            releaseLockCounter.incrementAndGet();
            assertNull(lockIdentifier);
          }

          @Override
          public Duration releaseIn(Job job) {
            return Duration.ofMinutes(10);
          }
        };

    lockMiddleware.handle(
        job,
        () -> {
          atomicInteger.incrementAndGet();
          return null;
        });
    verify(job, times(1)).release(JobStatus.FAILED, LockMiddleware.REASON, Duration.ofMinutes(10));
    assertEquals(1, releaseLockCounter.get());
    assertEquals(0, atomicInteger.get());
  }
}
