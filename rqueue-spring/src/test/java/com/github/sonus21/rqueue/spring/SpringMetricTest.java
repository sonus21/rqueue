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

package com.github.sonus21.rqueue.spring;

import static com.github.sonus21.rqueue.utils.RedisUtil.getRedisTemplate;

import com.github.sonus21.rqueue.core.RqueueMessage;
import com.github.sonus21.rqueue.exception.TimedOutException;
import com.github.sonus21.rqueue.spring.app.AppWithMetricEnabled;
import javax.annotation.PostConstruct;
import lombok.extern.slf4j.Slf4j;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.springframework.test.context.web.WebAppConfiguration;
import rqueue.test.tests.MetricTestBase;

@ContextConfiguration(classes = AppWithMetricEnabled.class)
@RunWith(SpringJUnit4ClassRunner.class)
@Slf4j
@WebAppConfiguration
public class SpringMetricTest extends MetricTestBase {
  static {
    System.setProperty("TEST_NAME", SpringMetricTest.class.getSimpleName());
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
