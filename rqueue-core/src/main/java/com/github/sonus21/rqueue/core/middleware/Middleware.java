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

import com.github.sonus21.rqueue.config.SimpleRqueueListenerContainerFactory;
import com.github.sonus21.rqueue.core.Job;
import com.github.sonus21.rqueue.core.support.MessageProcessor;
import com.github.sonus21.rqueue.models.enums.JobStatus;
import java.io.Serializable;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.Callable;

/**
 * Middlewares are used in a chain, at the end of chain listener method is called.
 *
 * <p>Each middleware is called in the order they have been added to Container/ContainerFactory
 *
 * <p>Any middleware can skip the next middleware call, skipping next middleware call means no
 * further processing of this job, in such case this middleware must either release(put back) or
 * delete this job using one of the methods {@link Job#release(JobStatus, Serializable)},
 * {@link Job#release(JobStatus, Serializable, Duration)}
 * {@link Job#delete(JobStatus, Serializable)}.
 *
 * <p>For example three middlewares [m1,m2,m3] are registered than m1 would be called first
 * followed by m2, m3 and message handler {@link HandlerMiddleware}. Middleware m1 can either call
 * m2 or skip it, skipping call of m2 means this job will not be processed by either m2 or m3 and
 * this job must be either released or deleted. If m2 is called from m1 than m2 can either call m3
 * or skip it, if m2 is skipping m3 call than it must either release or delete this job. If m3 is
 * called than m3 can either call the next middleware or skip it, the next middleware for m3 would
 * the {@link HandlerMiddleware} that would call the listener method, again if m3 is skipping
 * handler call than it should either release or delete this job.
 *
 * <p><b>NOTE:</b> Middlewares only called when preprocessor returns true.
 *
 * <p>It's recommended to use either preprocessor {@link
 * SimpleRqueueListenerContainerFactory#setPreExecutionMessageProcessor(MessageProcessor)} or
 * middlewares feature{@link SimpleRqueueListenerContainerFactory#setMiddlewares(List)}.
 *
 * @see LoggingMiddleware
 * @see ProfilerMiddleware
 * @see ContextMiddleware
 * @see PermissionMiddleware
 * @see LockMiddleware
 * @see RedisLockMiddleware
 */
public interface Middleware {

  /**
   * Middleware handles that would be called
   *
   * @param job  job object
   * @param next next middleware in chain
   * @throws Exception any exception
   */
  void handle(Job job, Callable<Void> next) throws Exception;
}
