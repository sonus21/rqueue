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
import java.util.concurrent.Callable;

/**
 * Permission middleware allows to check whether some permission is there for a given job,
 *
 * <p>If the given job does not have the right permission than this job would be released back to
 * pool for other consumers.
 *
 * <p><b>NOTE:</b> User's role was changed between and the said is no longer be able to perform
 * some action
 */
public interface PermissionMiddleware extends TimeProviderMiddleware {

  String REASON = "PermissionDenied";

  /**
   * Checks whether given job has enough permission to execute
   *
   * @param job job that's going to be executed
   * @return true/false
   */
  boolean hasPermission(Job job);

  @Override
  default void handle(Job job, Callable<Void> next) throws Exception {
    if (hasPermission(job)) {
      next.call();
    } else {
      job.release(JobStatus.FAILED, REASON, releaseIn(job));
    }
  }
}
