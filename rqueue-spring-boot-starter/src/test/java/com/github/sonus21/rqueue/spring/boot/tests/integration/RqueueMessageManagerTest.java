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

import com.github.sonus21.junit.SpringTestTracerExtension;
import com.github.sonus21.rqueue.core.EndpointRegistry;
import com.github.sonus21.rqueue.exception.TimedOutException;
import com.github.sonus21.rqueue.listener.QueueDetail;
import com.github.sonus21.rqueue.spring.boot.application.Application;
import com.github.sonus21.rqueue.test.common.SpringTestBase;
import com.github.sonus21.rqueue.test.dto.Notification;
import com.github.sonus21.rqueue.utils.TimeoutUtils;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestPropertySource;

@ExtendWith(SpringTestTracerExtension.class)
@ContextConfiguration(classes = Application.class)
@SpringBootTest
@Slf4j
@TestPropertySource(properties = {"use.system.redis=false", "spring.redis.port:8014"})
class RqueueMessageManagerTest extends SpringTestBase {
  @Test
  void testDeleteAll() throws TimedOutException {
    QueueDetail queueDetail = EndpointRegistry.get(notificationQueue);
    enqueue(notificationQueue, Notification.newInstance());
    enqueueIn(notificationQueue, Notification.newInstance(), 1000000);
    enqueueIn(Notification.newInstance(), queueDetail.getProcessingQueueName(), 1000_000);
    rqueueMessageManager.deleteAllMessages(notificationQueue);
    TimeoutUtils.waitFor(
        () -> getMessageCount(notificationQueue) == 0, "all messages to be deleted");
  }
}
