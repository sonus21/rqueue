/*
 * Copyright (c)  2019-2019, Sonu Kumar
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *       https://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */

package com.github.sonus21.rqueue.spring.boot.integration;

import static com.github.sonus21.rqueue.utils.TimeUtil.waitFor;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import com.github.sonus21.rqueue.exception.TimedOutException;
import com.github.sonus21.rqueue.producer.RqueueMessageSender;
import com.github.sonus21.rqueue.spring.boot.integration.app.dto.EmailTask;
import com.github.sonus21.rqueue.spring.boot.integration.app.service.ConsumedMessageService;
import java.util.ArrayList;
import java.util.List;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(classes = ApplicationWithAutoStartSchedulerDisabled.class)
public class ApplicationWithAutoStartSchedulerDisabledTest {
  @Autowired private ConsumedMessageService consumedMessageService;
  @Autowired private RqueueMessageSender messageSender;

  @Value("${email.queue.name}")
  private String emailQueue;

  @Test
  public void testPublishMessageIsTriggeredOnMessageAddition()
      throws InterruptedException, TimedOutException {
    int messageCount = 20;
    List<EmailTask> emailTasks = new ArrayList<>();
    List<String> ids = new ArrayList<>();
    for (int i = 0; i < messageCount; i++) {
      EmailTask emailTask = EmailTask.newInstance();
      messageSender.put(emailQueue, emailTask, 1000L);
      emailTasks.add(emailTask);
      ids.add(emailTask.getId());
    }
    Thread.sleep(5000);
    messageSender.put(emailQueue, EmailTask.newInstance());
    waitFor(
        () -> 1 == messageSender.getAllMessages(emailQueue).size(),
        20000L,
        "messages to be consumed");
    assertEquals(messageCount, consumedMessageService.getMessages(ids, EmailTask.class).size());
    assertTrue(
        emailTasks.containsAll(consumedMessageService.getMessages(ids, EmailTask.class).values()));
  }
}
