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
import java.io.Serializable;
import java.time.Duration;

/**
 * Middlewares are used in a chain, at the end of chain listener method is called.
 *
 * <p>Each middleware is called in the order they have been added to Container/ContainerFactory
 *
 * <p>Any middleware can skip the next middleware call, skipping next middleware call means no
 * other
 * processing of this job would be done, in such case this middleware must either release(put back)
 * or delete this job using one of the methods {@link Job#release(JobStatus, Serializable)}, {@link
 * Job#release(JobStatus, Serializable, Duration)} {@link Job#delete(JobStatus, Serializable)}.
 *
 * <p>For example three middlewares [m1,m2,m3] are registered than m1 would be called first
 * followed
 * by m2, m3 and message handler {@link HandlerMiddleware}. Middleware m1 can either call m2 or skip
 * it, skipping call of m2 means this job will not be processed by either m2 or m3 and this job must
 * be either released or deleted. If m2 is called from m1 than m2 can either call m3 or skip it, if
 * m2 is skipping m3 call than it must either release or delete this job. If m3 is called than m3
 * can either call the next middleware or skip it, the next middleware for m3 would the {@link
 * HandlerMiddleware} that would call the listener method, again if m3 is skipping handler call than
 * it should either release or delete this job.
 *
 * @see ContextMiddleware
 * @see LoggingMiddleware
 * @see PermissionMiddleware
 * @see RedisLockMiddleware
 * @see LockMiddleware
 */
public abstract class Middleware {

  private Middleware next;

  public void setNext(Middleware middleware) {
    if (next == null) {
      this.next = middleware;
    } else {
      throw new IllegalStateException("next handler must be null");
    }
  }

  public abstract void handle(Job job);

  protected void handleNext(Job job) {
    if (next != null) {
      next.handle(job);
    }
  }
}
