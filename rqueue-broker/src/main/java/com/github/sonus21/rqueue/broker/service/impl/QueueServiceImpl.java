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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.github.sonus21.rqueue.broker.dao.QueueStore;
import com.github.sonus21.rqueue.broker.models.db.QueueConfig;
import com.github.sonus21.rqueue.broker.models.request.BatchMessageEnqueueRequest;
import com.github.sonus21.rqueue.broker.models.request.CreateQueueRequest;
import com.github.sonus21.rqueue.broker.models.request.DeleteQueueRequest;
import com.github.sonus21.rqueue.broker.models.request.MessageEnqueueRequest;
import com.github.sonus21.rqueue.broker.models.request.MessageRequest;
import com.github.sonus21.rqueue.broker.models.request.Queue;
import com.github.sonus21.rqueue.broker.models.request.QueueWithPriority;
import com.github.sonus21.rqueue.broker.models.request.UpdateQueueRequest;
import com.github.sonus21.rqueue.broker.models.response.CreateQueueResponse;
import com.github.sonus21.rqueue.broker.models.response.DeleteQueueResponse;
import com.github.sonus21.rqueue.broker.models.response.IdResponse;
import com.github.sonus21.rqueue.broker.models.response.MessageEnqueueResponse;
import com.github.sonus21.rqueue.broker.models.response.BatchMessageResponse;
import com.github.sonus21.rqueue.broker.models.response.UpdateQueueResponse;
import com.github.sonus21.rqueue.broker.service.QueueService;
import com.github.sonus21.rqueue.common.RqueueLockManager;
import com.github.sonus21.rqueue.config.RqueueConfig;
import com.github.sonus21.rqueue.core.RqueueMessage;
import com.github.sonus21.rqueue.core.RqueueMessageTemplate;
import com.github.sonus21.rqueue.exception.ErrorCode;
import com.github.sonus21.rqueue.exception.LockException;
import com.github.sonus21.rqueue.exception.ProcessingException;
import com.github.sonus21.rqueue.exception.ValidationException;
import com.github.sonus21.rqueue.models.enums.EventType;
import com.github.sonus21.rqueue.utils.PriorityUtils;
import com.github.sonus21.rqueue.utils.StringUtils;
import com.github.sonus21.rqueue.web.service.RqueueMessageConverter;
import com.github.sonus21.rqueue.web.service.RqueueRedisMessagePublisher;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.util.CollectionUtils;

@Service
public class QueueServiceImpl implements QueueService {
  private final RqueueLockManager rqueueLockManager;
  private final QueueStore queueStore;
  private final RqueueConfig rqueueConfig;
  private final RqueueRedisMessagePublisher rqueueRedisMessagePublisher;
  private final RqueueMessageConverter rqueueMessageConverter;
  private final RqueueMessageTemplate rqueueMessageTemplate;

  @Autowired
  public QueueServiceImpl(
      RqueueLockManager rqueueLockManager,
      QueueStore queueStore,
      RqueueConfig rqueueConfig,
      RqueueRedisMessagePublisher rqueueRedisMessagePublisher,
      RqueueMessageConverter rqueueMessageConverter,
      RqueueMessageTemplate rqueueMessageTemplate) {
    this.rqueueLockManager = rqueueLockManager;
    this.queueStore = queueStore;
    this.rqueueConfig = rqueueConfig;
    this.rqueueRedisMessagePublisher = rqueueRedisMessagePublisher;
    this.rqueueMessageConverter = rqueueMessageConverter;
    this.rqueueMessageTemplate = rqueueMessageTemplate;
  }

  private RqueueMessage createMessage(MessageEnqueueRequest request) throws ProcessingException {
    String msg;
    try {
      msg = rqueueMessageConverter.fromMessage(request.getMessage());
    } catch (JsonProcessingException e) {
      throw new ProcessingException(e);
    }
    String queueName = request.getQueue().getName();
    if (request.getQueue().getPriority() != null) {
      queueName =
          PriorityUtils.getQueueNameForPriority(queueName, request.getQueue().getPriority());
    }
    return new RqueueMessage(queueName, msg, request.getRetryCount(), request.getDelay());
  }

  @Override
  public CreateQueueResponse create(CreateQueueRequest request)
      throws LockException, ValidationException, ProcessingException {
    if (rqueueLockManager.acquireLock(rqueueConfig.getQueuesKey(), Duration.ofSeconds(5))) {
      List<Queue> queues = queueStore.getAllQueue();
      for (Queue queue : queues) {
        for (Queue newQueue : request.getQueues()) {
          if (queue.getName().equals(newQueue.getName())) {
            rqueueLockManager.releaseLock(rqueueConfig.getQueuesKey());
            throw new ValidationException(ErrorCode.QUEUE_ALREADY_EXIST);
          }
        }
      }
      queueStore.addQueue(request.getQueues());

      rqueueRedisMessagePublisher.publishBrokerQueue(EventType.ADD, queues);
      return new CreateQueueResponse();
    }
    throw new LockException("Queue lock cannot be acquired");
  }

  @Override
  public UpdateQueueResponse update(UpdateQueueRequest request)
      throws ValidationException, LockException, ProcessingException {
    if (CollectionUtils.isEmpty(request.getPriority()) && request.getVisibilityTimeout() == null) {
      throw new ValidationException(ErrorCode.QUEUE_UPDATE_PARAMETERS_MISSING);
    }
    if (rqueueLockManager.acquireLock(rqueueConfig.getQueuesKey(), Duration.ofSeconds(5))) {
      if (queueStore.isQueueExist(request)) {
        QueueConfig queueConfig = queueStore.getConfig(request);
        boolean updateRequired = false;
        if (request.getVisibilityTimeout() != null
            && request.getVisibilityTimeout() != queueConfig.getVisibilityTimeout()) {
          updateRequired = true;
        }
        Map<String, Integer> userPriority =
            PriorityUtils.getUserPriority(queueConfig.getPriority());
        if (!CollectionUtils.isEmpty(request.getPriority())) {
          Map<String, Integer> priority = request.getPriority();
          for (Entry<String, Integer> entry : priority.entrySet()) {
            Integer val = userPriority.get(entry.getKey());
            if (!entry.getValue().equals(val)) {
              updateRequired = true;
              userPriority.put(entry.getKey(), entry.getValue());
            }
          }
          List<String> keysToBeRemoved = new ArrayList<>(4);
          for (Entry<String, Integer> entry : userPriority.entrySet()) {
            if (!priority.containsKey(entry.getKey())) {
              keysToBeRemoved.add(entry.getKey());
            }
          }
          if (!keysToBeRemoved.isEmpty()) {
            updateRequired = true;
            keysToBeRemoved.forEach(userPriority::remove);
          }
        }
        if (!updateRequired) {
          rqueueLockManager.releaseLock(rqueueConfig.getQueuesKey());
          throw new ValidationException(ErrorCode.NOTHING_TO_UPDATE);
        }
        queueStore.update(request, queueConfig, request.getVisibilityTimeout(), userPriority);
        rqueueRedisMessagePublisher.publishBrokerQueue(EventType.UPDATE, (Queue) request);
        rqueueLockManager.releaseLock(rqueueConfig.getQueuesKey());
        return new UpdateQueueResponse();
      }
      rqueueLockManager.releaseLock(rqueueConfig.getQueuesKey());
      throw new ValidationException(ErrorCode.QUEUE_DOES_NOT_EXIST);
    }
    throw new LockException("Queue lock cannot be acquired");
  }

  @Override
  public DeleteQueueResponse delete(DeleteQueueRequest request)
      throws ProcessingException, ValidationException, LockException {
    if (rqueueLockManager.acquireLock(rqueueConfig.getQueuesKey(), Duration.ofSeconds(5))) {
      if (queueStore.isQueueExist(request)) {
        queueStore.delete(request);
        rqueueRedisMessagePublisher.publishBrokerQueue(EventType.DELETION, request);
        rqueueLockManager.releaseLock(rqueueConfig.getQueuesKey());
        return new DeleteQueueResponse();
      }
      rqueueLockManager.releaseLock(rqueueConfig.getQueuesKey());
      throw new ValidationException(ErrorCode.QUEUE_DOES_NOT_EXIST);
    }
    throw new LockException("Queue lock cannot be acquired");
  }

  private String getScheduledQueueName(QueueWithPriority queue) {
    String queueName = rqueueConfig.getDelayedQueueName(queue.getName());
    if (!StringUtils.isEmpty(queue.getPriority())) {
      return PriorityUtils.getQueueNameForPriority(queueName, queue.getPriority());
    }
    return queueName;
  }

  private String getSimpleQueueName(QueueWithPriority queue) {
    String queueName = rqueueConfig.getQueueName(queue.getName());
    if (!StringUtils.isEmpty(queue.getPriority())) {
      return PriorityUtils.getQueueNameForPriority(queueName, queue.getPriority());
    }
    return queueName;
  }

  @Override
  public MessageEnqueueResponse enqueue(BatchMessageEnqueueRequest request)
      throws ProcessingException {
    List<IdResponse> responses = new ArrayList<>();
    for (MessageEnqueueRequest messageEnqueueRequest : request.getMessages()) {
      QueueWithPriority queue = messageEnqueueRequest.getQueue();
      QueueConfig queueConfig = queueStore.getConfig(queue);
      if (queueConfig == null) {
        responses.add(new IdResponse(ErrorCode.QUEUE_DOES_NOT_EXIST));
      } else {
        if (!StringUtils.isEmpty(queue.getPriority())
            && !queueConfig.isValidPriority(queue.getPriority())) {
          responses.add(new IdResponse(ErrorCode.INVALID_QUEUE_PRIORITY));
        } else {
          RqueueMessage message = createMessage(messageEnqueueRequest);
          responses.add(new IdResponse(message.getId()));
          if (messageEnqueueRequest.getDelay() != null) {
            String queueName = getScheduledQueueName(queue);
            rqueueMessageTemplate.addToZset(queueName, message, message.getProcessAt());
          } else {
            String queueName = getSimpleQueueName(queue);
            rqueueMessageTemplate.addMessage(queueName, message);
          }
        }
      }
    }
    return new MessageEnqueueResponse(responses);
  }

  @Override
  public BatchMessageResponse dequeue(MessageRequest messageRequest) throws ValidationException {
    QueueWithPriority queue = messageRequest.getQueue();
    QueueConfig queueConfig = queueStore.getConfig(queue);
    if(queueConfig == null){
      throw new ValidationException(ErrorCode.QUEUE_DOES_NOT_EXIST);
    }
    if(!StringUtils.isEmpty(queue.getPriority()) && !queueConfig.isValidPriority(queue.getPriority())){
      throw new ValidationException(ErrorCode.INVALID_QUEUE_PRIORITY);
    }
    //String queueName,
    //      String processingQueueName,
    //      String processingChannelName,
    //      long visibilityTimeout,
    //      int n
    List<RqueueMessage> messages = rqueueMessageTemplate.popN(queueConfig.getSimpleQueue(),
        queueConfig.getProcessingQueue(), queueConfig.getProcessingQueue())
    return null;
  }
}
