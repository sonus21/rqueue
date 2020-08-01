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

package com.github.sonus21.rqueue.common.impl;

import static org.springframework.util.Assert.isTrue;

import com.github.sonus21.rqueue.common.RqueueLockManager;
import com.github.sonus21.rqueue.common.RqueueRedisTemplate;
import java.time.Duration;
import org.springframework.util.StringUtils;

public class RqueueLockManagerImpl implements RqueueLockManager {
  private final RqueueRedisTemplate<String> redisTemplate;

  public RqueueLockManagerImpl(RqueueRedisTemplate<String> redisTemplate) {
    this.redisTemplate = redisTemplate;
  }

  @Override
  public boolean acquireLock(String lockKey, Duration duration) {
    isTrue(!StringUtils.isEmpty(lockKey), "key cannot be null.");
    Boolean result = redisTemplate.setIfAbsent(lockKey, lockKey, duration);
    return Boolean.TRUE.equals(result);
  }

  @Override
  public boolean releaseLock(String lockKey) {
    isTrue(!StringUtils.isEmpty(lockKey), "key cannot be null.");
    Boolean result = redisTemplate.delete(lockKey);
    return Boolean.TRUE.equals(result);
  }
}
