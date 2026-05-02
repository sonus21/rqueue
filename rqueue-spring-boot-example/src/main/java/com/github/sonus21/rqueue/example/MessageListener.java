/*
 * Copyright (c) 2019-2026 Sonu Kumar
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

package com.github.sonus21.rqueue.example;

import com.github.sonus21.rqueue.annotation.RqueueHandler;
import com.github.sonus21.rqueue.annotation.RqueueListener;
import com.github.sonus21.rqueue.core.RqueueMessageManager;
import com.github.sonus21.rqueue.listener.RqueueMessageHeaders;
import com.github.sonus21.rqueue.utils.TimeoutUtils;
import java.util.Random;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Component;

@Component
@Slf4j
public class MessageListener extends BaseListener {

  @RqueueListener(value = "${rqueue.simple.queue}")
  public void onSimpleMessage(String message) {
    execute("simple: {}", message, false);
  }

  @RqueueListener(
      value = {"${rqueue.delay.queue}", "${rqueue.delay2.queue}"},
      numRetries = "${rqueue.delay.queue.retries}",
      visibilityTimeout = "60*60*1000")
  public void onMessage(String message) {
    execute("delay: {}", message, true);
  }


  @RqueueListener(
      value = "sch-job-queue",
      deadLetterQueue = "job-morgue",
      numRetries = "2",
      deadLetterQueueListenerEnabled = "false",
      concurrency = "1-3")
  public void onSchJobMessage(Job job, @Header(RqueueMessageHeaders.ID) String messageId) {
    execute("sch-job-queue: {}", job, false);
    count += 1;
    if (count == 4) {
      boolean result = rqueueMessageManager.deleteMessage("sch-job-queue", messageId);
      log.info("Message {}  delete result is {}", messageId, result);
    }
  }

  @RqueueListener(value = "job-morgue", numRetries = "1", concurrency = "1-3")
  public void onJobDlqMessage(Job job) {
    execute("job-morgue: {}", job, true);
  }
}
