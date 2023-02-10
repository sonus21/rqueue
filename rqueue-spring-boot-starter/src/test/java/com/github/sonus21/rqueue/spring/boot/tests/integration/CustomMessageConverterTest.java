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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.github.sonus21.rqueue.core.RqueueMessage;
import com.github.sonus21.rqueue.exception.TimedOutException;
import com.github.sonus21.rqueue.spring.boot.application.ApplicationWithCustomMessageConverter;
import com.github.sonus21.rqueue.spring.boot.tests.SpringBootIntegrationTest;
import com.github.sonus21.rqueue.test.dto.Job;
import com.github.sonus21.rqueue.test.tests.BasicListenerTest;
import com.github.sonus21.rqueue.utils.Constants;
import com.github.sonus21.rqueue.utils.SerializationUtils;
import com.github.sonus21.rqueue.utils.TimeoutUtils;
import java.util.ArrayList;
import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestPropertySource;

@ContextConfiguration(classes = ApplicationWithCustomMessageConverter.class)
@SpringBootTest
@Slf4j
@TestPropertySource(
    properties = {
        "rqueue.retry.per.poll=1000",
        "spring.data.redis.port=8010",
        "reservation.request.active=true",
        "mysql.db.name=CustomMessageConverterTest",
        "use.system.redis=false",
        "monitor.enabled=true",
        "rqueue.message.converter.provider.class=com.github.sonus21.rqueue.test.util.TestMessageConverterProvider",
    })
@SpringBootIntegrationTest
class CustomMessageConverterTest extends BasicListenerTest {

  @Test
  void verifyListenerIsWorking() throws TimedOutException {
    verifySimpleTaskExecution();
  }

  @Test
  void verifyMessageStoredInDb() throws TimedOutException, JsonProcessingException {
    rqueueEndpointManager.pauseUnpauseQueue(jobQueue, true);
    TimeoutUtils.sleep(Constants.ONE_MILLI);
    List<Job> jobs = new ArrayList<>();
    enqueue(
        jobQueue,
        (i) -> {
          Job job = Job.newInstance();
          jobs.add(job);
          return job;
        },
        10,
        false);
    List<RqueueMessage> rqueueMessageList = rqueueMessageManager.getAllRqueueMessage(jobQueue);
    assertEquals(jobs.size(), rqueueMessageList.size());
    for (RqueueMessage message : rqueueMessageList) {
      String rawMessage = message.getMessage();
      assertTrue(SerializationUtils.isJson(rawMessage));
      Job job = objectMapper.readValue(rawMessage, Job.class);
      assertTrue(job.isValid());
      boolean found = false;
      for (Job j : jobs) {
        if (j.equals(job)) {
          found = true;
          break;
        }
      }
      assertTrue(found);
    }
    rqueueEndpointManager.pauseUnpauseQueue(jobQueue, false);
    deleteAllMessages(jobQueue);
  }
}
