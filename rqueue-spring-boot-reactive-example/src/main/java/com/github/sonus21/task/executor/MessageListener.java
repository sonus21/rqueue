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

package com.github.sonus21.task.executor;

import com.github.sonus21.rqueue.annotation.RqueueListener;
import com.github.sonus21.rqueue.utils.TimeoutUtils;
import java.util.Random;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

@Component
@Slf4j
public class MessageListener {

  private static final Random random = new Random();

  @Value("${delay.queue.fail.percentage:0}")
  private int percentageFailure;

  private boolean shouldFail() {
    if (percentageFailure == 0) {
      return false;
    }
    if (percentageFailure >= 100) {
      return true;
    }
    return random.nextInt(100) < percentageFailure;
  }

  private void execute(String msg, Object any) {
    log.info(msg, any);
    TimeoutUtils.sleep(random.nextInt(2000));
  }

  @RqueueListener(value = "${rqueue.simple.queue}", active = "true")
  public void consumeMessage(String message) {
    execute("simple: {}", message);
  }

  @RqueueListener(
      value = {"${rqueue.delay.queue}", "${rqueue.delay2.queue}"},
      numRetries = "${rqueue.delay.queue.retries}",
      visibilityTimeout = "60*60*1000",
      active = "true")
  public void onMessage(String message) {
    execute("delay: {}", message);
    if (shouldFail()) {
      throw new NullPointerException("Failing On Purpose " + message);
    }
  }

  @RqueueListener(
      value = "job-queue",
      deadLetterQueue = "job-morgue",
      numRetries = "2",
      deadLetterQueueListenerEnabled = "false",
      concurrency = "1-3",
      active = "true")
  public void onMessage(Job job) {
    execute("job-queue: {}", job);
    if (shouldFail()) {
      throw new IllegalStateException("OMG!" + job);
    }
  }

  @RqueueListener(value = "job-morgue", numRetries = "1", concurrency = "1-3")
  public void onMessageJobDlq(Job job) {
    execute("job-morgue: {}", job);
    if (shouldFail()) {
      throw new IllegalStateException("OMG!" + job);
    }
  }
}
