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
import com.github.sonus21.rqueue.core.context.Context;
import com.github.sonus21.rqueue.core.context.DefaultContext;
import java.util.concurrent.Callable;

/**
 * Context middleware allows us to set context for the running job, context could be any thing like
 * putting user information for permission check, initiating some transaction.
 *
 * <p>Job's context can be updated from any where/middleware, this middleware is just for
 * consolidation action.
 *
 * @see DefaultContext
 */
public interface ContextMiddleware extends Middleware {

  /**
   * Return non null context for this job. This method can also update MDC {@link org.slf4j.MDC}
   * context or any other context .
   *
   * @param job job object
   * @return a new context
   */
  Context getContext(Job job);

  @Override
  default void handle(Job job, Callable<Void> next) throws Exception {
    Context context = getContext(job);
    if (context == null) {
      throw new IllegalStateException("Returned context is null");
    }
    job.setContext(context);
    next.call();
  }
}
