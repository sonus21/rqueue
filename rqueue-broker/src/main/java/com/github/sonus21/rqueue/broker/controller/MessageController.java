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

import com.github.sonus21.rqueue.broker.dto.request.AckRequest;
import com.github.sonus21.rqueue.broker.dto.request.BatchMessageEnqueueRequest;
import com.github.sonus21.rqueue.broker.dto.request.BatchMessagePublishRequest;
import com.github.sonus21.rqueue.broker.dto.request.ExtendTimeoutRequest;
import com.github.sonus21.rqueue.broker.dto.request.MessageRequest;
import com.github.sonus21.rqueue.broker.dto.response.AckResponse;
import com.github.sonus21.rqueue.broker.dto.response.ExtendTimeoutResponse;
import com.github.sonus21.rqueue.broker.dto.response.MessageEnqueueResponse;
import com.github.sonus21.rqueue.broker.dto.response.MessagePublishResponse;
import com.github.sonus21.rqueue.broker.dto.response.MessageResponse;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

@RestController("api/v1/message")
public class MessageController {
  @PostMapping("dequeue")
  public MessageResponse getMessage(@RequestBody MessageRequest messageRequest) {
    return null;
  }

  @PostMapping("ack")
  public AckResponse ack(@RequestBody AckRequest request) {
    return null;
  }

  @PostMapping("extend-timeout")
  public ExtendTimeoutResponse extendVisibilityTimeout(@RequestBody ExtendTimeoutRequest request) {
    return null;
  }

  @PostMapping("enqueue")
  public MessageEnqueueResponse enqueue(@RequestBody BatchMessageEnqueueRequest request) {
    return null;
  }

  @PostMapping("publish")
  public MessagePublishResponse publish(@RequestBody BatchMessagePublishRequest request) {
    return null;
  }
}
