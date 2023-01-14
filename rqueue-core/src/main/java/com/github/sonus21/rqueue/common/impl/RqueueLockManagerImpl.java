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

package com.github.sonus21.rqueue.common.impl;

import com.github.sonus21.rqueue.common.RqueueLockManager;
import com.github.sonus21.rqueue.dao.RqueueStringDao;
import java.time.Duration;
import org.springframework.util.Assert;

public class RqueueLockManagerImpl implements RqueueLockManager {

  private final RqueueStringDao rqueueStringDao;

  public RqueueLockManagerImpl(RqueueStringDao rqueueStringDao) {
    this.rqueueStringDao = rqueueStringDao;
  }

  @Override
  public boolean acquireLock(String lockKey, String lockValue, Duration duration) {
    Assert.hasText(lockKey, "key cannot be null.");
    Assert.hasText(lockValue, "value cannot be null.");
    Boolean result = rqueueStringDao.setIfAbsent(lockKey, lockValue, duration);
    return Boolean.TRUE.equals(result);
  }

  @Override
  public boolean releaseLock(String lockKey, String lockValue) {
    Assert.hasText(lockKey, "key cannot be null.");
    Assert.hasText(lockValue, "value cannot be null.");
    Boolean result = rqueueStringDao.deleteIfSame(lockKey, lockValue);
    return Boolean.TRUE.equals(result);
  }
}
