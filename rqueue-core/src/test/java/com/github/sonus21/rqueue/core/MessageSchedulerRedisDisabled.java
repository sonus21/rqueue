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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.Mockito.doReturn;

import com.github.sonus21.rqueue.config.RqueueSchedulerConfig;
import com.github.sonus21.rqueue.core.DelayedMessageSchedulerTest.TestThreadPoolScheduler;
import com.github.sonus21.rqueue.listener.QueueDetail;
import com.github.sonus21.rqueue.models.event.RqueueBootstrapEvent;
import com.github.sonus21.rqueue.utils.TestUtils;
import com.github.sonus21.rqueue.utils.ThreadUtils;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.data.redis.core.RedisTemplate;

@ExtendWith(MockitoExtension.class)
public class MessageSchedulerRedisDisabled {
  @Mock private RqueueSchedulerConfig rqueueSchedulerConfig;
  @Mock private RedisTemplate<String, Long> redisTemplate;

  @InjectMocks private DelayedMessageScheduler messageScheduler = new DelayedMessageScheduler();

  private String slowQueue = "slow-queue";
  private QueueDetail slowQueueDetail = TestUtils.createQueueDetail(slowQueue);

  @BeforeEach
  public void init() {
    MockitoAnnotations.openMocks(this);
    EndpointRegistry.delete();
    EndpointRegistry.register(slowQueueDetail);
  }

  @Test
  public void startShouldSubmitsTaskWhenRedisIsDisabled() throws Exception {
    doReturn(1).when(rqueueSchedulerConfig).getDelayedMessageThreadPoolSize();
    TestThreadPoolScheduler scheduler = new TestThreadPoolScheduler();
    try (MockedStatic<ThreadUtils> threadUtils = Mockito.mockStatic(ThreadUtils.class)) {
      threadUtils
          .when(() -> ThreadUtils.createTaskScheduler(1, "delayedMessageScheduler-", 60))
          .thenReturn(scheduler);
      messageScheduler.onApplicationEvent(new RqueueBootstrapEvent("Test", true));
      assertEquals(1, scheduler.tasks.size());
      assertNull(FieldUtils.readField(messageScheduler, "messageSchedulerListener", true));
      messageScheduler.destroy();
    }
  }
}
