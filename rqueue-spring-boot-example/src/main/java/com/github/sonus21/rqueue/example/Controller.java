/*
 * Copyright 2020 Sonu Kumar
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

package com.github.sonus21.rqueue.example;

import com.github.sonus21.rqueue.core.RqueueMessageSender;
import java.time.Duration;
import java.util.List;
import java.util.UUID;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.connection.stream.Consumer;
import org.springframework.data.redis.connection.stream.MapRecord;
import org.springframework.data.redis.connection.stream.StreamOffset;
import org.springframework.data.redis.connection.stream.StreamReadOptions;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.StreamOperations;
import org.springframework.util.CollectionUtils;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
@AllArgsConstructor(onConstructor = @__(@Autowired))
@Slf4j
public class Controller {
  private RqueueMessageSender rqueueMessageSender;
  private RedisTemplate<String, String> redisTemplate;

  @GetMapping(value = "/push")
  public String push(
      String q,
      String msg,
      @RequestParam(required = false) Integer numRetries,
      @RequestParam(required = false) Long delay,
      @RequestParam List<String> val) {
    if (numRetries == null && delay == null) {
      rqueueMessageSender.enqueue(q, msg);
    } else if (numRetries == null) {
      rqueueMessageSender.enqueueIn(q, msg, delay);
    } else {
      rqueueMessageSender.enqueueInWithRetry(q, msg, numRetries, delay);
    }
    log.info("Message {}, val {}", msg, val);
    return "Message sent successfully";
  }

  @GetMapping("job")
  public String sendJobNotification() {
    Job job = new Job();
    job.setId(UUID.randomUUID().toString());
    job.setMessage("Hi this is " + job.getId());
    rqueueMessageSender.enqueue("job-queue", job);
    log.info("{}", job);
    return job.toString();
  }

  @GetMapping("job-delay")
  public String sendJobNotificationWithDelay() {
    Job job = new Job();
    job.setId(UUID.randomUUID().toString());
    job.setMessage("Hi this is " + job.getId());
    rqueueMessageSender.enqueueIn("job-queue", job, 2000L);
    return job.toString();
  }

  class LinkStatus {
    private final boolean updated;

    LinkStatus(boolean updated) {
      this.updated = updated;
    }
  }
  // Parameter studioLinkingID refers to the StudioLinking that the requester wants to monitor
  // updateList is a unique token to track individual consumers in Redis
  @GetMapping("/subscribe-to-updates/{linkId}/{updatesId}")
  public LinkStatus subscribeToUpdates(
      @PathVariable("linkId") Integer linkId, @PathVariable("updatesId") Integer updatesId) {
    StreamOperations<String, String, String> op = redisTemplate.opsForStream();
    Consumer consumer = Consumer.from("test", "test");
    // auto ack block stream read with size 1
    StreamReadOptions readOptions = StreamReadOptions.empty().block(Duration.ofSeconds(5)).count(1);
    List<MapRecord<String, String, String>> records =
        op.read(consumer, readOptions, StreamOffset.latest("my-stream"));
    return new LinkStatus(!CollectionUtils.isEmpty(records));
  }
}
