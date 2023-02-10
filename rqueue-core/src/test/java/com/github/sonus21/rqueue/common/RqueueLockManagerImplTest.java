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

package com.github.sonus21.rqueue.common;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.doReturn;

import com.github.sonus21.TestBase;
import com.github.sonus21.rqueue.CoreUnitTest;
import com.github.sonus21.rqueue.common.impl.RqueueLockManagerImpl;
import com.github.sonus21.rqueue.dao.RqueueStringDao;
import java.time.Duration;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

@CoreUnitTest
class RqueueLockManagerImplTest extends TestBase {

  private final String lockKey = "test-key";
  private final String lockValue = "test-value";
  @Mock
  private RqueueStringDao rqueueStringDao;
  private RqueueLockManager rqueueLockManager;

  @BeforeEach
  public void init() {
    MockitoAnnotations.openMocks(this);
    rqueueLockManager = new RqueueLockManagerImpl(rqueueStringDao);
  }

  @Test
  void acquireLock() {
    assertFalse(rqueueLockManager.acquireLock(lockKey, lockValue, Duration.ofSeconds(5)));
    doReturn(Boolean.FALSE)
        .when(rqueueStringDao)
        .setIfAbsent(lockKey, lockValue, Duration.ofSeconds(5));
    assertFalse(rqueueLockManager.acquireLock(lockKey, lockValue, Duration.ofSeconds(5)));
    doReturn(Boolean.TRUE)
        .when(rqueueStringDao)
        .setIfAbsent(lockKey, lockValue, Duration.ofSeconds(1));
    assertTrue(rqueueLockManager.acquireLock(lockKey, lockValue, Duration.ofSeconds(1)));
  }

  @Test
  void releaseLock() {
    assertFalse(rqueueLockManager.releaseLock(lockKey, lockValue));
    doReturn(Boolean.FALSE).when(rqueueStringDao).deleteIfSame(lockKey, lockValue);
    assertFalse(rqueueLockManager.releaseLock(lockKey, lockValue));
    doReturn(Boolean.TRUE).when(rqueueStringDao).deleteIfSame(lockKey, lockValue);
    assertTrue(rqueueLockManager.releaseLock(lockKey, lockValue));
  }
}
