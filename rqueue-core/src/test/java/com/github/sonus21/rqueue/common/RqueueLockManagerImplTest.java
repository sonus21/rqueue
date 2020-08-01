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

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;

import com.github.sonus21.rqueue.common.impl.RqueueLockManagerImpl;
import java.time.Duration;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.StrictStubs.class)
public class RqueueLockManagerImplTest {
  private RqueueRedisTemplate<String> template = mock(RqueueRedisTemplate.class);
  private RqueueLockManager rqueueLockManager = new RqueueLockManagerImpl(template);
  private String lockKey = "test-key";

  @Test
  public void acquireLock() {
    assertFalse(rqueueLockManager.acquireLock(lockKey, Duration.ofSeconds(5)));
    doReturn(Boolean.FALSE).when(template).setIfAbsent(lockKey, lockKey, Duration.ofSeconds(5));
    assertFalse(rqueueLockManager.acquireLock(lockKey, Duration.ofSeconds(5)));
    doReturn(Boolean.TRUE).when(template).setIfAbsent(lockKey, lockKey, Duration.ofSeconds(1));
    assertTrue(rqueueLockManager.acquireLock(lockKey, Duration.ofSeconds(1)));
  }

  @Test
  public void releaseLock() {
    assertFalse(rqueueLockManager.releaseLock(lockKey));
    doReturn(Boolean.FALSE).when(template).delete(lockKey);
    assertFalse(rqueueLockManager.releaseLock(lockKey));
    doReturn(Boolean.TRUE).when(template).delete(lockKey);
    assertTrue(rqueueLockManager.releaseLock(lockKey));
  }
}
