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

package com.github.sonus21.rqueue.spring.boot.tests.integration;

import com.github.sonus21.rqueue.exception.TimedOutException;
import com.github.sonus21.rqueue.spring.boot.application.Application;
import lombok.extern.slf4j.Slf4j;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import rqueue.test.tests.MetricTestBase;

@SpringBootTest
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(classes = Application.class)
@Slf4j
@TestPropertySource(
    properties = {
      "auto.start.scheduler=false",
      "spring.redis.port=6384",
      "mysql.db.name=test4",
      "rqueue.metrics.count.failure=true",
      "rqueue.metrics.count.execution=true",
    })
public class MetricsTest extends MetricTestBase {
  static {
    System.setProperty("TEST_NAME", MetricsTest.class.getSimpleName());
  }

  @Test
  public void delayedQueueStatus() throws TimedOutException {
    this.delayedQueueStatus(redisTemplate);
  }

  @Test
  public void metricStatus() throws TimedOutException {
    this.metricStatus(redisTemplate);
  }

  @Test
  public void countStatusTest() throws TimedOutException {
    this.countStatus();
  }
}
