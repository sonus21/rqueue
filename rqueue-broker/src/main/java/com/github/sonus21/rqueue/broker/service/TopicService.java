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

package com.github.sonus21.rqueue.broker.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.github.sonus21.rqueue.broker.models.request.BatchMessagePublishRequest;
import com.github.sonus21.rqueue.broker.models.request.CreateTopicRequest;
import com.github.sonus21.rqueue.broker.models.request.DeleteTopicRequest;
import com.github.sonus21.rqueue.broker.models.request.SubscriptionRequest;
import com.github.sonus21.rqueue.broker.models.request.SubscriptionUpdateRequest;
import com.github.sonus21.rqueue.broker.models.request.UnsubscriptionRequest;
import com.github.sonus21.rqueue.broker.models.response.CreateTopicResponse;
import com.github.sonus21.rqueue.broker.models.response.DeleteTopicResponse;
import com.github.sonus21.rqueue.broker.models.response.MessagePublishResponse;
import com.github.sonus21.rqueue.broker.models.response.SubscriptionResponse;
import com.github.sonus21.rqueue.broker.models.response.SubscriptionUpdateResponse;
import com.github.sonus21.rqueue.broker.models.response.UnsubscriptionResponse;
import com.github.sonus21.rqueue.exception.LockException;
import com.github.sonus21.rqueue.exception.ProcessingException;
import com.github.sonus21.rqueue.exception.ValidationException;

public interface TopicService {
  CreateTopicResponse create(CreateTopicRequest request) throws LockException, ValidationException;

  SubscriptionUpdateResponse update(SubscriptionUpdateRequest request)
      throws ValidationException, ProcessingException, LockException;

  DeleteTopicResponse delete(DeleteTopicRequest request)
      throws LockException, ValidationException, ProcessingException;

  SubscriptionResponse subscribe(SubscriptionRequest request)
      throws ValidationException, ProcessingException, LockException;

  UnsubscriptionResponse unsubscribe(UnsubscriptionRequest request)
      throws ValidationException, LockException, ProcessingException;

  MessagePublishResponse publish(BatchMessagePublishRequest request)
      throws ValidationException, JsonProcessingException;
}
