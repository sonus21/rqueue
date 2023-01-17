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

package com.github.sonus21.rqueue.spring.boot.tests.integration;

import com.github.sonus21.rqueue.exception.TimedOutException;
import com.github.sonus21.rqueue.spring.boot.application.BootMetricApplication;
import com.github.sonus21.rqueue.spring.boot.tests.SpringBootIntegrationTest;
import com.github.sonus21.rqueue.test.tests.MetricTest;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestPropertySource;

@SpringBootTest
@ContextConfiguration(classes = BootMetricApplication.class)
@Slf4j
@TestPropertySource(
    properties = {
        "rqueue.retry.per.poll=20",
        "rqueue.scheduler.auto.start=false",
        "spring.data.redis.port=8003",
        "mysql.db.name=test4",
        "rqueue.metrics.count.failure=true",
        "rqueue.metrics.count.execution=true",
        "sms.queue.active=true"
    })
@SpringBootIntegrationTest
@EnabledIfEnvironmentVariable(named = "CI", matches = "true")
@Tag("redisCluster")
class BootMetricsTest extends MetricTest {

  @Test
  void scheduledQueueStatus() throws TimedOutException {
    this.verifyScheduledQueueStatus();
  }

  @Test
  void metricStatus() throws TimedOutException {
    this.verifyMetricStatus();
  }

  @Test
  void countStatusTest() throws TimedOutException {
    this.verifyCountStatus();
  }

  @Test
  void simpleQueueMessageCount() throws TimedOutException {
    verifyNotificationQueueMessageCount();
  }

  @Test
  void priorityQueueMessageCount() throws TimedOutException {
    verifySmsQueueMessageCount();
  }
}
