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

package com.github.sonus21.rqueue.config;

import lombok.Getter;
import lombok.Setter;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;

@Configuration
@Getter
@Setter
public class RqueueWebConfig {
  /**
   * Control whether web app is enabled or not. If it's marked false then it will throw HTTP 503
   * (Service unavailable) error.
   */
  @Value("${rqueue.web.enable:true}")
  private boolean enable;

  @Value("${rqueue.web.url.prefix:}")
  private String urlPrefix;

  @Value("${rqueue.web.max.message.move.count:1000}")
  private int maxMessageMoveCount;

  /**
   * Whether queue stats should be collected or not. When this flag is disabled, metric data won't
   * be available in the dashboard. This consumes heavy CPU resources as well due to statistics
   * aggregation and computations.
   */
  @Value("${rqueue.web.collect.listener.stats:true}")
  private boolean collectListenerStats;

  @Value("${rqueue.web.collect.listener.stats.thread.count:1}")
  private int statsAggregatorThreadCount;

  // 3 months
  @Value("${rqueue.web.statistic.history.day:90}")
  private int historyDay;

  // number of jobs that should be aggregated at once
  @Value("${rqueue.web.collect.statistic.aggregate.event.count:500}")
  private int aggregateEventCount;

  // controls how to long to wait before doping aggregation
  @Value("${rqueue.web.collect.statistic.aggregate.event.wait.time:60}")
  private int aggregateEventWaitTimeInSecond;

  // controls how long to wait for the running threads to complete the execution
  @Value("${rqueue.web.collect.statistic.aggregate.shutdown.wait.time:500}")
  private int aggregateShutdownWaitTime;

  // how long  we should wait for the lock duration
  @Value("${rqueue.web.collect.statistic.aggregate.event.lock.duration:500}")
  private int aggregateEventLockDurationInMs;
}
