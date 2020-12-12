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

package com.github.sonus21.rqueue.common;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;

import com.github.sonus21.rqueue.common.impl.RqueueLockManagerImpl;
import com.github.sonus21.rqueue.dao.RqueueStringDao;
import java.time.Duration;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class RqueueLockManagerImplTest {
  private RqueueStringDao rqueueStringDao = mock(RqueueStringDao.class);
  private RqueueLockManager rqueueLockManager = new RqueueLockManagerImpl(rqueueStringDao);
  private String lockKey = "test-key";

  @Test
  public void acquireLock() {
    assertFalse(rqueueLockManager.acquireLock(lockKey, Duration.ofSeconds(5)));
    doReturn(Boolean.FALSE)
        .when(rqueueStringDao)
        .setIfAbsent(lockKey, lockKey, Duration.ofSeconds(5));
    assertFalse(rqueueLockManager.acquireLock(lockKey, Duration.ofSeconds(5)));
    doReturn(Boolean.TRUE)
        .when(rqueueStringDao)
        .setIfAbsent(lockKey, lockKey, Duration.ofSeconds(1));
    assertTrue(rqueueLockManager.acquireLock(lockKey, Duration.ofSeconds(1)));
  }

  @Test
  public void releaseLock() {
    assertFalse(rqueueLockManager.releaseLock(lockKey));
    doReturn(Boolean.FALSE).when(rqueueStringDao).delete(lockKey);
    assertFalse(rqueueLockManager.releaseLock(lockKey));
    doReturn(Boolean.TRUE).when(rqueueStringDao).delete(lockKey);
    assertTrue(rqueueLockManager.releaseLock(lockKey));
  }
}
