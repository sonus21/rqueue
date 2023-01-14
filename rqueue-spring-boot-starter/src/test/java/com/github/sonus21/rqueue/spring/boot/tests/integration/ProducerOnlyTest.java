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

package com.github.sonus21.rqueue.spring.boot.tests.integration;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.github.sonus21.rqueue.core.EndpointRegistry;
import com.github.sonus21.rqueue.spring.boot.application.ProducerOnlyApplication;
import com.github.sonus21.rqueue.spring.boot.tests.SpringBootIntegrationTest;
import com.github.sonus21.rqueue.test.dto.ChatIndexing;
import com.github.sonus21.rqueue.test.dto.Email;
import com.github.sonus21.rqueue.test.dto.FeedGeneration;
import com.github.sonus21.rqueue.test.dto.Job;
import com.github.sonus21.rqueue.test.dto.LongRunningJob;
import com.github.sonus21.rqueue.test.dto.Notification;
import com.github.sonus21.rqueue.test.dto.PeriodicJob;
import com.github.sonus21.rqueue.test.dto.Reservation;
import com.github.sonus21.rqueue.test.dto.ReservationRequest;
import com.github.sonus21.rqueue.test.dto.Sms;
import com.github.sonus21.rqueue.test.dto.UserBanned;
import com.github.sonus21.rqueue.test.tests.BasicListenerTest;
import com.github.sonus21.rqueue.utils.Constants;
import com.github.sonus21.rqueue.utils.PriorityUtils;
import java.util.Arrays;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestPropertySource;

@ContextConfiguration(classes = ProducerOnlyApplication.class)
@SpringBootTest
@Slf4j
@SpringBootIntegrationTest
@Tag("producerOnly")
@EnabledIfEnvironmentVariable(named = "CI", matches = "true")
@TestPropertySource(properties = {"rqueue.system.mode=PRODUCER"})
class ProducerOnlyTest extends BasicListenerTest {

  private void validateQueue(Object message, String queue) {
    EndpointRegistry.get(queue);
    enqueue(message, queue);
  }

  @Test
  void queueCount() {
    log.info("Rqueue System Mode {}", rqueueConfig.getMode());
    assertEquals(35, EndpointRegistry.getRegisteredQueueCount(), rqueueConfig.getMode().toString());
    for (int i = 0; i < 10; i++) {
      String queueName = "new_queue_" + i;
      // no listeners are attached so enqueue any thing
      validateQueue(Email.newInstance(), queueName);
      if (i % 3 == 0) {
        validateQueue(
            Email.newInstance(), PriorityUtils.getQueueNameForPriority(queueName, "high"));
        validateQueue(Email.newInstance(), PriorityUtils.getQueueNameForPriority(queueName, "low"));
      }
    }
    validateQueue(Notification.newInstance(), notificationQueue);
    validateQueue(Email.newInstance(), emailQueue);
    validateQueue(Job.newInstance(), jobQueue);
    validateQueue(Sms.newInstance(), smsQueue);
    validateQueue(Sms.newInstance(), PriorityUtils.getQueueNameForPriority(smsQueue, "low"));
    validateQueue(Sms.newInstance(), PriorityUtils.getQueueNameForPriority(smsQueue, "high"));
    validateQueue(Sms.newInstance(), PriorityUtils.getQueueNameForPriority(smsQueue, "critical"));
    validateQueue(Sms.newInstance(), PriorityUtils.getQueueNameForPriority(smsQueue, "medium"));
    validateQueue(Reservation.newInstance(), reservationQueue);
    validateQueue(ReservationRequest.newInstance(), reservationRequestQueue);
    validateQueue(PeriodicJob.newInstance(), periodicJobQueue);
    validateQueue(LongRunningJob.newInstance(Constants.ONE_MILLI), longRunningJobQueue);
    validateQueue(Arrays.asList(Email.newInstance()), listEmailQueue);
    validateQueue(ChatIndexing.newInstance(), chatIndexingQueue);
    validateQueue(FeedGeneration.newInstance(), feedGenerationQueue);
    validateQueue(UserBanned.newInstance(), userBannedQueue);
    validateQueue(ReservationRequest.newInstance(), reservationRequestDeadLetterQueue);
  }
}
