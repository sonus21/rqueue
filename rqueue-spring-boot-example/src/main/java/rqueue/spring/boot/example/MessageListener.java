/*
 * Copyright (c) 2019-2019, Sonu Kumar
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

package rqueue.spring.boot.example;

import com.github.sonus21.rqueue.annotation.RqueueListener;
import java.util.Random;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

@Component
@Slf4j
public class MessageListener {
  private Random random = new Random();

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

  @RqueueListener(value = "${rqueue.simple.queue}")
  public void consumeMessage(String message) {
    log.info("simple: {}, {}", message, message.getClass());
  }

  @RqueueListener(
      value = {"${rqueue.delay.queue}", "${rqueue.delay2.queue}"},
      delayedQueue = "${rqueue.delay.queue.delayed-queue}",
      numRetries = "${rqueue.delay.queue.retries}")
  public void onMessage(String message) {
    log.info("delay: {}", message);
    if (shouldFail()) {
      throw new NullPointerException(message);
    }
  }

  @RqueueListener(value = "job-queue", delayedQueue = "true")
  public void onMessage(Job job) {
    log.info("job-queue: {}", job);
  }
}
