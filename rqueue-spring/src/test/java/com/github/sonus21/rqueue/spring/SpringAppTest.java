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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import com.github.sonus21.rqueue.listener.QueueDetail;
import com.github.sonus21.rqueue.listener.RqueueMessageListenerContainer;
import com.github.sonus21.rqueue.spring.app.AppWithMetricEnabled;
import com.github.sonus21.test.RqueueSpringTestRunner;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.web.WebAppConfiguration;

@ContextConfiguration(classes = AppWithMetricEnabled.class)
@RunWith(RqueueSpringTestRunner.class)
@Slf4j
@WebAppConfiguration
public class SpringAppTest {
  @Autowired private RqueueMessageListenerContainer container;

  @Value("${email.queue.name}")
  private String emailQueue;

  @Value("${job.queue.name}")
  private String jobQueueName;

  @Value("${notification.queue.name}")
  private String notificationQueueName;

  @Test
  public void numListeners() {
    Map<String, QueueDetail> registeredQueue = container.getRegisteredQueues();
    assertEquals(3, registeredQueue.size());
    assertTrue(registeredQueue.containsKey(notificationQueueName));
    assertTrue(registeredQueue.containsKey(emailQueue));
    assertTrue(registeredQueue.containsKey(jobQueueName));
  }
}
