/*
 * Copyright (c) 2019-2023 Sonu Kumar
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

package com.github.sonus21.rqueue.test.dto;

import java.util.UUID;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;
import org.apache.commons.lang3.RandomStringUtils;

@Data
@NoArgsConstructor
@EqualsAndHashCode(callSuper = true)
public class PeriodicJob extends BaseQueueMessage {

  private String jobName;
  private int executionTime;

  public static PeriodicJob newInstance() {
    PeriodicJob job = new PeriodicJob();
    job.setId(UUID.randomUUID().toString());
    job.setJobName(RandomStringUtils.randomAlphabetic(10));
    job.setExecutionTime(1 + (int) (Math.random() * 5));
    return job;
  }
}
