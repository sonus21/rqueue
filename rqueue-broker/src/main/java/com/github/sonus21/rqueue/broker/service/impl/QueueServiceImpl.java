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

package com.github.sonus21.rqueue.broker.service.impl;

import com.github.sonus21.rqueue.broker.models.request.BatchMessageEnqueueRequest;
import com.github.sonus21.rqueue.broker.models.request.CreateQueueRequest;
import com.github.sonus21.rqueue.broker.models.request.DeleteQueueRequest;
import com.github.sonus21.rqueue.broker.models.request.MessageRequest;
import com.github.sonus21.rqueue.broker.models.request.UpdateQueueRequest;
import com.github.sonus21.rqueue.broker.models.response.CreateQueueResponse;
import com.github.sonus21.rqueue.broker.models.response.DeleteQueueResponse;
import com.github.sonus21.rqueue.broker.models.response.MessageEnqueueResponse;
import com.github.sonus21.rqueue.broker.models.response.MessageResponse;
import com.github.sonus21.rqueue.broker.models.response.UpdateQueueResponse;
import com.github.sonus21.rqueue.broker.service.QueueService;
import com.github.sonus21.rqueue.common.RqueueLockManager;
import com.github.sonus21.rqueue.config.RqueueConfig;
import com.github.sonus21.rqueue.core.RqueueMessageTemplate;
import com.github.sonus21.rqueue.web.dao.RqueueQStore;
import com.github.sonus21.rqueue.web.service.RqueueMessageConverter;
import com.github.sonus21.rqueue.web.service.RqueueRedisMessagePublisher;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service
public class QueueServiceImpl implements QueueService {
  private final RqueueConfig rqueueConfig;
  private final RqueueLockManager rqueueLockManager;
  private final RqueueQStore rqueueQStore;
  private final RqueueRedisMessagePublisher rqueueRedisMessagePublisher;
  private final RqueueMessageConverter rqueueMessageConverter;
  private final RqueueMessageTemplate rqueueMessageTemplate;

  @Autowired
  public QueueServiceImpl(
      RqueueConfig rqueueConfig,
      RqueueLockManager rqueueLockManager,
      RqueueQStore rqueueQStore,
      RqueueRedisMessagePublisher rqueueRedisMessagePublisher,
      RqueueMessageConverter rqueueMessageConverter,
      RqueueMessageTemplate rqueueMessageTemplate) {
    this.rqueueConfig = rqueueConfig;
    this.rqueueLockManager = rqueueLockManager;
    this.rqueueQStore = rqueueQStore;
    this.rqueueRedisMessagePublisher = rqueueRedisMessagePublisher;
    this.rqueueMessageConverter = rqueueMessageConverter;
    this.rqueueMessageTemplate = rqueueMessageTemplate;
  }

  @Override
  public CreateQueueResponse create(CreateQueueRequest request) {
    return null;
  }

  @Override
  public UpdateQueueResponse update(UpdateQueueRequest request) {
    return null;
  }

  @Override
  public DeleteQueueResponse delete(DeleteQueueRequest request) {
    return null;
  }

  @Override
  public MessageEnqueueResponse enqueue(BatchMessageEnqueueRequest request) {
    return null;
  }

  @Override
  public MessageResponse dequeue(MessageRequest messageRequest) {
    return null;
  }
}
