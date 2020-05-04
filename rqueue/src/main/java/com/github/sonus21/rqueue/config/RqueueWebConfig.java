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

  @Value("${rqueue.web.statistic.history.day:180}")
  private int historyDay;

  @Value("${rqueue.web.collect.statistic.aggregate.event.count:500}")
  private int aggregateEventCount;

  @Value("${rqueue.web.collect.statistic.aggregate.event.wait.time:60}")
  private int aggregateEventWaitTime;

  @Value("${rqueue.web.collect.statistic.aggregate.shutdown.wait.time:500}")
  private int aggregateShutdownWaitTime;
}
