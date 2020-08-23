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

package com.github.sonus21.rqueue.core;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.mockito.Mockito.doReturn;

import com.github.sonus21.rqueue.config.RqueueSchedulerConfig;
import com.github.sonus21.rqueue.core.DelayedMessageSchedulerTest.TestThreadPoolScheduler;
import com.github.sonus21.rqueue.listener.QueueDetail;
import com.github.sonus21.rqueue.models.event.RqueueBootstrapEvent;
import com.github.sonus21.rqueue.utils.TestUtils;
import com.github.sonus21.rqueue.utils.ThreadUtils;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;
import org.mockito.quality.Strictness;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.springframework.data.redis.core.RedisTemplate;

@RunWith(PowerMockRunner.class)
@PrepareForTest(fullyQualifiedNames = {"com.github.sonus21.rqueue.utils.ThreadUtils"})
public class MessageSchedulerRedisDisabled {
  @Rule public MockitoRule mockito = MockitoJUnit.rule().strictness(Strictness.STRICT_STUBS);

  @Mock private RqueueSchedulerConfig rqueueSchedulerConfig;
  @Mock private RedisTemplate<String, Long> redisTemplate;

  @InjectMocks private DelayedMessageScheduler messageScheduler = new DelayedMessageScheduler();

  private String slowQueue = "slow-queue";
  private QueueDetail slowQueueDetail = TestUtils.createQueueDetail(slowQueue);

  @Before
  public void init() {
    MockitoAnnotations.initMocks(this);
    EndpointRegistry.register(slowQueueDetail);
  }

  @Test
  public void startShouldSubmitsTaskWhenRedisIsDisabled() throws Exception {
    doReturn(1).when(rqueueSchedulerConfig).getDelayedMessageThreadPoolSize();
    TestThreadPoolScheduler scheduler = new TestThreadPoolScheduler();
    PowerMockito.stub(
            PowerMockito.method(
                ThreadUtils.class, "createTaskScheduler", Integer.TYPE, String.class, Integer.TYPE))
        .toReturn(scheduler);
    messageScheduler.onApplicationEvent(new RqueueBootstrapEvent("Test", true));
    assertEquals(1, scheduler.tasks.size());
    assertNull(FieldUtils.readField(messageScheduler, "messageSchedulerListener", true));
    messageScheduler.destroy();
  }
}
