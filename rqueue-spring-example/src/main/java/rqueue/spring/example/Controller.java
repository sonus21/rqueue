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

package rqueue.spring.example;

import com.github.sonus21.rqueue.core.RqueueMessageEnqueuer;
import com.github.sonus21.rqueue.utils.StringUtils;
import io.micrometer.prometheus.PrometheusMeterRegistry;
import java.util.UUID;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
@AllArgsConstructor(onConstructor = @__(@Autowired))
@Slf4j
public class Controller {

  private final RqueueMessageEnqueuer rqueueMessageEnqueuer;
  private final PrometheusMeterRegistry meterRegistry;

  @GetMapping("metric")
  public String getMetricStatus() {
    return meterRegistry.scrape();
  }

  @GetMapping(value = "/push")
  public String push(
      String q,
      String msg,
      @RequestParam(required = false) Integer numRetries,
      @RequestParam(required = false) Long delay) {
    if (numRetries == null && delay == null) {
      rqueueMessageEnqueuer.enqueue(q, msg);
    } else if (numRetries == null) {
      rqueueMessageEnqueuer.enqueueIn(q, msg, delay);
    } else {
      rqueueMessageEnqueuer.enqueueInWithRetry(q, msg, numRetries, delay);
    }
    log.info("Message {}", msg);
    return "Message sent successfully";
  }

  private String getQueue(String queue) {
    if (queue == null) {
      return "job-queue";
    }
    return queue;
  }

  private Job getJob(String message) {
    Job job = new Job();
    job.setId(UUID.randomUUID().toString());
    if (!StringUtils.isEmpty(message)) {
      job.setMessage(message);
    } else {
      job.setMessage("Hi this is " + job.getId());
    }
    return job;
  }

  @GetMapping("job")
  public String sendJobNotification(
      @RequestParam(required = false) String msg, @RequestParam(required = false) String q) {
    Job job = getJob(msg);
    rqueueMessageEnqueuer.enqueue(getQueue(q), job);
    log.info("{}", job);
    return job.toString();
  }

  @GetMapping("job-delay")
  public String sendJobNotificationWithDelay(
      @RequestParam(required = false) String q,
      @RequestParam(required = false) String msg,
      @RequestParam(required = false, defaultValue = "2000") long delay) {
    Job job = getJob(msg);
    rqueueMessageEnqueuer.enqueueIn(getQueue(q), job, delay);
    return job.toString();
  }
}
