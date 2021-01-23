/*
 *  Copyright 2021 Sonu Kumar
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *         https://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and limitations under the License.
 *
 */

package com.github.sonus21.rqueue.core.middleware;

import com.github.sonus21.rqueue.common.RqueueRedisTemplate;
import com.github.sonus21.rqueue.config.RqueueConfig;
import com.github.sonus21.rqueue.core.Job;
import java.time.Duration;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import org.springframework.data.redis.connection.RedisConnectionFactory;

/**
 * This uses Redis as backend for locking, using <b>SETNX</b> a lock is acquired.
 *
 * <p>An implementation of this class must implement getLockIdentifier, that should return lock
 * identifier for this job.
 */
public abstract class RedisLockMiddleware implements LockMiddleware {

  private final RqueueRedisTemplate<String> template;
  private final Set<String> acquiredLocks = ConcurrentHashMap.newKeySet();

  public RedisLockMiddleware(RedisConnectionFactory redisConnectionFactory) {
    template = new RqueueRedisTemplate<>(redisConnectionFactory);
  }

  /**
   * Returns lock identifier for this job, could be a simple job id, user id or any other depending
   * on the user case. This method must returns non null value.
   *
   * @param job job object
   * @return lock identifier
   */
  protected abstract String getLockIdentifier(Job job);

  /**
   * A lock can be required for forever or 1 second, that's all depends on the use case, bny default
   * a lock is acquired for visibility time.
   *
   * @param job job object
   * @return duration for this lock
   */
  protected Duration getLockDuration(Job job) {
    return job.getQueueDetail().visibilityTimeoutDuration();
  }

  @Override
  public void releaseLock(Job job, String lockIdentifier) {
    if (lockIdentifier == null) {
      return;
    }
    if (acquiredLocks.contains(lockIdentifier)) {
      template.delete(lockIdentifier);
      acquiredLocks.remove(lockIdentifier);
    }
  }

  @Override
  public String acquireLock(Job job) {
    String lockIdentifier = getLockIdentifier(job);
    if (acquiredLocks.contains(lockIdentifier)) {
      return null;
    }
    Duration lockDuration = getLockDuration(job);
    if (template.setIfAbsent(lockIdentifier, RqueueConfig.getBrokerId(), lockDuration)) {
      acquiredLocks.add(lockIdentifier);
      return lockIdentifier;
    }
    return null;
  }
}
