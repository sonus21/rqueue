/*
 * Copyright 2019 Sonu Kumar
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

package rqueue.spring.example;

import com.github.sonus21.rqueue.producer.RqueueMessageSender;
import io.micrometer.prometheus.PrometheusMeterRegistry;
import java.util.UUID;
import lombok.AllArgsConstructor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
@AllArgsConstructor(onConstructor = @__(@Autowired))
public class Controller {
  private RqueueMessageSender rqueueMessageSender;
  private PrometheusMeterRegistry meterRegistry;

  @GetMapping("metric")
  public String getMetricStatus() {
    return meterRegistry.scrape();
  }

  @GetMapping(value = "/push")
  public String getCities(
      String q,
      String msg,
      @RequestParam(required = false) Integer numRetries,
      @RequestParam(required = false) Long delay) {
    if (numRetries == null && delay == null) {
      rqueueMessageSender.put(q, msg);
    } else if (numRetries == null) {
      rqueueMessageSender.put(q, msg, delay);
    } else {
      rqueueMessageSender.put(q, msg, numRetries, delay);
    }
    return "Message sent successfully";
  }

  @GetMapping("job")
  public String sendJobNotification() {
    Job job = new Job();
    job.setId(UUID.randomUUID().toString());
    job.setMessage("Hi this is " + job.getId());
    rqueueMessageSender.put("job-queue", job);
    return job.toString();
  }

  @GetMapping("job-delay")
  public String sendJobDelayNotification() {
    Job job = new Job();
    job.setId(UUID.randomUUID().toString());
    job.setMessage("Hi this is " + job.getId());
    rqueueMessageSender.put("job-queue", job, 2000L);
    return job.toString();
  }
}
