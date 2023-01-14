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

import com.github.sonus21.rqueue.core.Job;
import com.github.sonus21.rqueue.models.enums.JobStatus;
import com.github.sonus21.rqueue.utils.StringUtils;
import java.util.concurrent.Callable;

/**
 * Lock middleware can be used for locking purpose Use cases:
 *
 * <p>User's wallet amount should not be updated in parallel, in that case a lock on user's wallet
 * account can be acquired to avoid parallel execution of task.
 *
 * <p>Only one task for a queue should be running at any point of time, in that case lock on the
 * given queue can be acquired.
 */
public interface LockMiddleware extends TimeProviderMiddleware {

  String REASON = "Lock could not be acquired";

  /**
   * This method should acquire lock from any source like Redis, File, Java Lock or any system and
   * returns lock identifier for the same. If it's unable to acquire lock than this method must
   * return null/empty string. If locking system does not support lock identifier than you can
   * return {@link Job#getId()}.
   *
   * @param job running job object
   * @return lock identifier
   */
  String acquireLock(Job job);

  /**
   * This method is called to release lock acquired using {@link #acquireLock(Job)}.
   *
   * <p><b> This method is always called </b>
   *
   * @param job            the running job
   * @param lockIdentifier lock identifier
   */
  void releaseLock(Job job, String lockIdentifier);

  @Override
  default void handle(Job job, Callable<Void> callable) throws Exception {
    String lockIdentifier = null;
    try {
      lockIdentifier = acquireLock(job);
      if (!StringUtils.isEmpty(lockIdentifier)) {
        callable.call();
      } else {
        job.release(JobStatus.FAILED, REASON, releaseIn(job));
      }
    } finally {
      releaseLock(job, lockIdentifier);
    }
  }
}
