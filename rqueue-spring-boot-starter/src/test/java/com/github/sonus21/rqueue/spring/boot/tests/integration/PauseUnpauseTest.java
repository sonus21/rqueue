/*
 * Copyright (c) 2021-2023 Sonu Kumar
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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.github.sonus21.AtomicValueHolder;
import com.github.sonus21.rqueue.exception.TimedOutException;
import com.github.sonus21.rqueue.models.request.PauseUnpauseQueueRequest;
import com.github.sonus21.rqueue.spring.boot.application.Application;
import com.github.sonus21.rqueue.spring.boot.tests.SpringBootIntegrationTest;
import com.github.sonus21.rqueue.test.PauseUnpauseEventListener;
import com.github.sonus21.rqueue.test.common.SpringTestBase;
import com.github.sonus21.rqueue.test.dto.Notification;
import com.github.sonus21.rqueue.utils.Constants;
import com.github.sonus21.rqueue.utils.TimeoutUtils;
import com.github.sonus21.rqueue.web.service.RqueueUtilityService;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestPropertySource;

@ContextConfiguration(classes = Application.class)
@SpringBootTest
@Slf4j
@TestPropertySource(
    properties = {
        "rqueue.retry.per.poll=1",
        "spring.data.redis.port=8021",
        "mysql.db.name=PauseUnpauseTest",
        "use.system.redis=false",
    })
@SpringBootIntegrationTest
@Tag("redisCluster")
class PauseUnpauseTest extends SpringTestBase {

  @Autowired
  private PauseUnpauseEventListener eventListener;
  @Autowired
  private RqueueUtilityService rqueueUtilityService;

  @Test
  void onMessageNotification() throws TimedOutException {
    enqueue(notificationQueue, (i) -> Notification.newInstance(), 500, false);
    TimeoutUtils.waitFor(
        () -> consumedMessageStore.getConsumedMessagesForQueue(notificationQueue).size() > 10,
        "10 messages to be consumed");

    log.info("Requesting to pause queue {}", notificationQueue);
    PauseUnpauseQueueRequest pauseRequest = new PauseUnpauseQueueRequest(true);
    pauseRequest.setName(notificationQueue);
    rqueueUtilityService.pauseUnpauseQueue(pauseRequest);
    TimeoutUtils.waitFor(() -> eventListener.getEventList().size() == 1, "pause event");
    TimeoutUtils.sleep(Constants.ONE_MILLI);
    int messageCount = consumedMessageStore.getConsumedMessagesForQueue(notificationQueue).size();
    AtomicValueHolder<Integer> holder = new AtomicValueHolder<>(messageCount);
    TimeoutUtils.waitFor(
        () -> {
          int newCount = consumedMessageStore.getConsumedMessagesForQueue(notificationQueue).size();
          boolean eq = holder.get() == newCount;
          holder.set(newCount);
          return eq;
        },
        "message consumer to stopped");
    TimeoutUtils.sleep(Constants.ONE_MILLI);
    messageCount = consumedMessageStore.getConsumedMessagesForQueue(notificationQueue).size();
    assertEquals(holder.get(), messageCount);

    log.info("Re-request to pause queue {}", pauseRequest);
    rqueueUtilityService.pauseUnpauseQueue(pauseRequest);
    TimeoutUtils.sleep(Constants.MIN_DELAY);
    TimeoutUtils.waitFor(
        () -> eventListener.getEventList().size() == 1, "pause event should not emit");

    pauseRequest.setPause(false);
    log.info("Unpause request {}", pauseRequest);
    rqueueUtilityService.pauseUnpauseQueue(pauseRequest);
    TimeoutUtils.sleep(Constants.MIN_DELAY);
    TimeoutUtils.waitFor(() -> eventListener.getEventList().size() == 2, "unpause event");

    TimeoutUtils.waitFor(
        () -> {
          int newCount = consumedMessageStore.getConsumedMessagesForQueue(notificationQueue).size();
          boolean neq = holder.get() != newCount;
          holder.set(newCount);
          return neq;
        },
        "message consumer to start");
    assertNotEquals(messageCount, holder.get());
    assertTrue(eventListener.getEventList().get(0).isPaused());
    assertEquals(notificationQueue, eventListener.getEventList().get(0).getQueue());
    assertFalse(eventListener.getEventList().get(1).isPaused());
    assertEquals(notificationQueue, eventListener.getEventList().get(1).getQueue());

    log.info("Duplicate unpause request {}", pauseRequest);
    rqueueUtilityService.pauseUnpauseQueue(pauseRequest);
    TimeoutUtils.sleep(Constants.MIN_DELAY);
    TimeoutUtils.waitFor(() -> eventListener.getEventList().size() == 2, "duplicate unpause event");
    System.out.println("Test has finished");
  }
}
