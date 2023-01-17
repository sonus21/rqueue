/*
 * Copyright (c) 2020-2023 Sonu Kumar
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

package com.github.sonus21.rqueue.config;

import com.github.sonus21.rqueue.utils.HttpUtils;
import lombok.AccessLevel;
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

  /**
   * Base path for Rqueue web endpoints. Relative to server.servlet.context-path
   */
  @Value("${rqueue.web.url.prefix:}")
  @Getter(AccessLevel.NONE)
  private String urlPrefix;

  @Value("${server.servlet.context-path:}")
  private String servletContextPath;

  @Value("${rqueue.web.max.message.move.count:1000}")
  private int maxMessageMoveCount;

  /**
   * Whether queue stats should be collected or not. When this flag is disabled, metric data won't
   * be available in the dashboard.
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

  // controls how long to wait before doing aggregation
  @Value("${rqueue.web.collect.statistic.aggregate.event.wait.time:60}")
  private int aggregateEventWaitTimeInSecond;

  // controls how long, application should wait for the running threads to complete the execution
  @Value("${rqueue.web.collect.statistic.aggregate.shutdown.wait.time:500}")
  private int aggregateShutdownWaitTime;

  // lock duration for aggregate job, acquired  per queue
  @Value("${rqueue.web.collect.statistic.aggregate.event.lock.duration:500}")
  private int aggregateEventLockDurationInMs;

  public String getUrlPrefix(String xForwardedPrefix) {
    return HttpUtils.joinPath(xForwardedPrefix, servletContextPath, urlPrefix);
  }
}
