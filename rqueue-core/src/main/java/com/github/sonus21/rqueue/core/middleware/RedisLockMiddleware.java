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

public abstract class RedisLockMiddleware extends LockMiddleware {

  public static final String REASON = "Lock could not be acquired";
  private final RqueueRedisTemplate<String> template;
  private final Set<String> acquiredLocks = ConcurrentHashMap.newKeySet();

  public RedisLockMiddleware(RedisConnectionFactory redisConnectionFactory) {
    template = new RqueueRedisTemplate<>(redisConnectionFactory);
  }

  protected abstract String getLockIdentifier(Job job);

  protected Duration getLockDuration(Job job) {
    return job.getQueueDetail().visibilityTimeoutDuration();
  }

  public void releaseLock(Job job, String lockIdentifier) {
    if (lockIdentifier == null) {
      return;
    }
    if (acquiredLocks.contains(lockIdentifier)) {
      template.delete(lockIdentifier);
      acquiredLocks.remove(lockIdentifier);
    }
  }

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
