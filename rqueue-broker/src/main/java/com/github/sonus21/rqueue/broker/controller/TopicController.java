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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.github.sonus21.rqueue.broker.models.request.BatchMessagePublishRequest;
import com.github.sonus21.rqueue.broker.models.request.CreateTopicRequest;
import com.github.sonus21.rqueue.broker.models.request.SubscriptionRequest;
import com.github.sonus21.rqueue.broker.models.request.SubscriptionUpdateRequest;
import com.github.sonus21.rqueue.broker.models.request.UnsubscriptionRequest;
import com.github.sonus21.rqueue.broker.models.response.CreateTopicResponse;
import com.github.sonus21.rqueue.broker.models.response.MessagePublishResponse;
import com.github.sonus21.rqueue.broker.models.response.SubscriptionResponse;
import com.github.sonus21.rqueue.broker.models.response.SubscriptionUpdateResponse;
import com.github.sonus21.rqueue.broker.models.response.UnsubscriptionResponse;
import com.github.sonus21.rqueue.broker.service.TopicService;
import com.github.sonus21.rqueue.exception.LockException;
import com.github.sonus21.rqueue.exception.ProcessingException;
import com.github.sonus21.rqueue.exception.ValidationException;
import javax.validation.Valid;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

@RestController("api/v1/topic")
public class TopicController {
  private final TopicService topicService;

  @Autowired
  public TopicController(TopicService topicService) {
    this.topicService = topicService;
  }

  @PostMapping("new")
  public CreateTopicResponse newTopic(@RequestBody @Valid CreateTopicRequest request)
      throws LockException, ValidationException {
    return topicService.create(request);
  }

  @PostMapping("subscribe")
  public SubscriptionResponse subscribe(@RequestBody @Valid SubscriptionRequest request)
      throws ProcessingException, LockException, ValidationException {
    return topicService.subscribe(request);
  }

  @PutMapping("subscribe")
  public SubscriptionUpdateResponse updateSubscription(
      @RequestBody @Valid SubscriptionUpdateRequest request)
      throws ProcessingException, LockException, ValidationException {
    return topicService.update(request);
  }

  @PostMapping("unsubscribe")
  public UnsubscriptionResponse unsubscribe(@RequestBody UnsubscriptionRequest request)
      throws ProcessingException, LockException, ValidationException {
    return topicService.unsubscribe(request);
  }

  @PostMapping("publish")
  public MessagePublishResponse publish(@RequestBody BatchMessagePublishRequest request)
      throws ValidationException, JsonProcessingException {
    return topicService.publish(request);
  }
}
