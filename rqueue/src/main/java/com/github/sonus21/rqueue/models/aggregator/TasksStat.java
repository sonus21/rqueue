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

package com.github.sonus21.rqueue.models.aggregator;

import com.github.sonus21.rqueue.models.db.JobRunTime;
import lombok.ToString;

@ToString
public class TasksStat {
  public long discarded = 0;
  public long success = 0;
  public long movedToDlq = 0;
  public long retried = 0;
  public long jobCount;
  public long minExecution = Long.MAX_VALUE;
  public long maxExecution = 0;
  public long totalExecutionTime;

  public JobRunTime jobRunTime() {
    return new JobRunTime(minExecution, maxExecution, jobCount, totalExecutionTime);
  }
}
