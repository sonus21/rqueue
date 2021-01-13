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

import com.github.sonus21.rqueue.core.Job;
import com.github.sonus21.rqueue.models.enums.JobStatus;

public abstract class LockMiddleware extends TimeProviderMiddleware {

  public static final String REASON = "Lock could not be acquired";

  public abstract String acquireLock(Job job);

  public abstract void releaseLock(Job job, String lockIdentifier);

  @Override
  public void handle(Job job) {
    String lockIdentifier = null;
    try {
      lockIdentifier = acquireLock(job);
      if (lockIdentifier != null) {
        handleNext(job);
      } else {
        job.release(JobStatus.FAILED, REASON, releaseIn(job));
      }
    } finally {
      releaseLock(job, lockIdentifier);
    }
  }
}
