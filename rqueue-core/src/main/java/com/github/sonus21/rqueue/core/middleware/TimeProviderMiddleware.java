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
import com.github.sonus21.rqueue.utils.Constants;
import com.github.sonus21.rqueue.utils.backoff.TaskExecutionBackOff;
import java.time.Duration;

/**
 * A middleware extension that allows implementation to provide different types of delay in
 * different case.
 *
 * @see TaskExecutionBackOff
 */
public interface TimeProviderMiddleware extends Middleware {

  Duration ONE_SECOND = Duration.ofMillis(Constants.ONE_MILLI);

  /**
   * A middleware can dispose the current job or put it back to queue. While putting back, a
   * middleware can decide to put this job in queue immediately or in 5 seconds or so. Releasing job
   * immediately has side effect when job queue is empty. Job can reappear to the queue as soon as
   * it is put back in the queue, so the delay should be large enough to handle parallel execution
   * otherwise it can create loop. By default job is released to queue in one second.
   *
   * <p>Another implementation can use {@link TaskExecutionBackOff} to provide this delay
   *
   * @param job the current job
   * @return time duration
   */
  default Duration releaseIn(Job job) {
    return ONE_SECOND;
  }
}
