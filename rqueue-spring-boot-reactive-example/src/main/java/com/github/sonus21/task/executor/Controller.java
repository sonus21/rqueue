/*
 *  Copyright 2021 Sonu Kumar
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *         https://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and limitations under the License.
 *
 */

package com.github.sonus21.task.executor;

import com.github.sonus21.rqueue.core.ReactiveRqueueMessageEnqueuer;
import java.util.UUID;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Mono;

@RestController
@AllArgsConstructor(onConstructor = @__(@Autowired))
@Slf4j
public class Controller {

  private final ReactiveRqueueMessageEnqueuer reactiveRqueueMessageEnqueuer;

  @GetMapping(value = "/push")
  public Mono<String> push(
      String q,
      String msg,
      @RequestParam(required = false) Integer numRetries,
      @RequestParam(required = false) Long delay) {
    Mono<String> data;
    if (numRetries == null && delay == null) {
      data = reactiveRqueueMessageEnqueuer.enqueue(q, msg);
    } else if (numRetries == null) {
      data = reactiveRqueueMessageEnqueuer.enqueueIn(q, msg, delay);
    } else {
      data = reactiveRqueueMessageEnqueuer.enqueueInWithRetry(q, msg, numRetries, delay);
    }
    log.info("Message {}", msg);
    return data;
  }

  @GetMapping("job")
  public Mono<String> sendJobNotification() {
    Job job = new Job();
    job.setId(UUID.randomUUID().toString());
    job.setMessage("Hi this is " + job.getId());
    Mono<String> mono = reactiveRqueueMessageEnqueuer.enqueue("job-queue", job);
    log.info("{}", job);
    return mono;
  }

  @GetMapping("job-delay")
  public Mono<String> sendJobNotificationWithDelay() {
    Job job = new Job();
    job.setId(UUID.randomUUID().toString());
    job.setMessage("Hi this is " + job.getId());
    Mono<String> mono = reactiveRqueueMessageEnqueuer.enqueueIn("job-queue", job, 2000L);
    log.info("{}", job);
    return mono;
  }
}
