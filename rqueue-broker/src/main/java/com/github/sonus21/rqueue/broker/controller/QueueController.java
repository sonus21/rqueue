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

package com.github.sonus21.rqueue.broker.controller;

import com.github.sonus21.rqueue.broker.models.request.BatchMessageEnqueueRequest;
import com.github.sonus21.rqueue.broker.models.request.CreateQueueRequest;
import com.github.sonus21.rqueue.broker.models.request.MessageRequest;
import com.github.sonus21.rqueue.broker.models.response.CreateQueueResponse;
import com.github.sonus21.rqueue.broker.models.response.MessageEnqueueResponse;
import com.github.sonus21.rqueue.broker.models.response.MessageResponse;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

@RestController("api/v1/queue")
public class QueueController {
  @PostMapping("new")
  public CreateQueueResponse newQueue(@RequestBody CreateQueueRequest request) {
    return null;
  }

  @PostMapping("enqueue")
  public MessageEnqueueResponse enqueue(@RequestBody BatchMessageEnqueueRequest request) {
    return null;
  }

  @PostMapping("dequeue")
  public MessageResponse getMessage(@RequestBody MessageRequest messageRequest) {
    return null;
  }
}
