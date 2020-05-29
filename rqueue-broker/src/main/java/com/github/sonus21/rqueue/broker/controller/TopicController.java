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

import com.github.sonus21.rqueue.broker.dto.request.SubscriptionRequest;
import com.github.sonus21.rqueue.broker.dto.request.TopicRequest;
import com.github.sonus21.rqueue.broker.dto.response.TopicResponse;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

@RestController("api/v1/topic")
public class TopicController {
  public TopicResponse topic(@RequestBody TopicRequest request) {
    return null;
  }

  @PostMapping("subscribe")
  public TopicResponse subscribe(@RequestBody SubscriptionRequest request) {
    return null;
  }

  @PostMapping("subscribe")
  public TopicResponse subscribe(@RequestBody SubscriptionRequest request) {
    return null;
  }


}
