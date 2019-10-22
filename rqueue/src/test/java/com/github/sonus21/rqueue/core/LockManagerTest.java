/*
 * Copyright (c)  2019-2019, Sonu Kumar
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *       https://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */

package com.github.sonus21.rqueue.core;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.util.concurrent.TimeUnit;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;
import org.mockito.stubbing.Answer;

@RunWith(MockitoJUnitRunner.StrictStubs.class)
public class LockManagerTest {
  private StringMessageTemplate stringMessageTemplate = mock(StringMessageTemplate.class);
  private LockManager lockManager = new LockManager(stringMessageTemplate);
  private String lock = "lock";

  @Test
  public void acquireLockThrowException() {
    doAnswer(
            (Answer<Boolean>)
                invocation -> {
                  throw new Exception("Some error occur");
                })
        .when(stringMessageTemplate)
        .putIfAbsent(lock, 10L, TimeUnit.SECONDS);
    assertFalse(lockManager.acquireLock(lock));
  }

  @Test
  public void acquireLockFalse() {
    doAnswer(
            (Answer<Boolean>)
                invocation -> {
                  return true;
                })
        .when(stringMessageTemplate)
        .putIfAbsent(lock, 10L, TimeUnit.SECONDS);
    assertTrue(lockManager.acquireLock(lock));
  }

  @Test
  public void releaseLock() {
    lockManager.releaseLock(lock);
    verify(stringMessageTemplate, times(1)).delete(lock);
  }
}
