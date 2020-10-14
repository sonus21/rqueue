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

import com.github.sonus21.rqueue.broker.dao.TopicStore;
import com.github.sonus21.rqueue.broker.models.db.SubscriptionConfig;
import com.github.sonus21.rqueue.broker.models.db.TopicConfig;
import com.github.sonus21.rqueue.broker.models.request.CreateTopicRequest;
import com.github.sonus21.rqueue.broker.models.request.DeleteTopicRequest;
import com.github.sonus21.rqueue.broker.models.request.MessagePublishRequest;
import com.github.sonus21.rqueue.broker.models.request.MessagePushRequest;
import com.github.sonus21.rqueue.broker.models.request.Observer;
import com.github.sonus21.rqueue.broker.models.request.Subscription;
import com.github.sonus21.rqueue.broker.models.request.SubscriptionRequest;
import com.github.sonus21.rqueue.broker.models.request.SubscriptionUpdateRequest;
import com.github.sonus21.rqueue.broker.models.request.Topic;
import com.github.sonus21.rqueue.broker.models.request.UnsubscriptionRequest;
import com.github.sonus21.rqueue.broker.models.response.CreateTopicResponse;
import com.github.sonus21.rqueue.broker.models.response.DeleteTopicResponse;
import com.github.sonus21.rqueue.broker.models.response.IdResponse;
import com.github.sonus21.rqueue.broker.models.response.MessagePublishResponse;
import com.github.sonus21.rqueue.broker.models.response.SubscriptionResponse;
import com.github.sonus21.rqueue.broker.models.response.SubscriptionUpdateResponse;
import com.github.sonus21.rqueue.broker.models.response.UnsubscriptionResponse;
import com.github.sonus21.rqueue.broker.service.TopicService;
import com.github.sonus21.rqueue.common.RqueueLockManager;
import com.github.sonus21.rqueue.config.RqueueConfig;
import com.github.sonus21.rqueue.core.RqueueMessage;
import com.github.sonus21.rqueue.core.RqueueMessageTemplate;
import com.github.sonus21.rqueue.exception.ErrorCode;
import com.github.sonus21.rqueue.exception.LockException;
import com.github.sonus21.rqueue.exception.ProcessingException;
import com.github.sonus21.rqueue.exception.ValidationException;
import com.github.sonus21.rqueue.models.enums.EventType;
import com.github.sonus21.rqueue.web.service.RqueueMessageConverter;
import com.github.sonus21.rqueue.web.service.RqueueRedisMessagePublisher;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.util.CollectionUtils;

@Service
public class TopicServiceImpl implements TopicService {
  private final RqueueConfig rqueueConfig;
  private final RqueueLockManager rqueueLockManager;
  private final TopicStore topicStore;
  private final RqueueRedisMessagePublisher rqueueRedisMessagePublisher;
  private final RqueueMessageConverter rqueueMessageConverter;
  private final RqueueMessageTemplate rqueueMessageTemplate;

  @Autowired
  public TopicServiceImpl(
      RqueueConfig rqueueConfig,
      RqueueLockManager rqueueLockManager,
      TopicStore topicStore,
      RqueueRedisMessagePublisher rqueueRedisMessagePublisher,
      RqueueMessageConverter rqueueMessageConverter,
      RqueueMessageTemplate rqueueMessageTemplate) {
    this.rqueueConfig = rqueueConfig;
    this.rqueueLockManager = rqueueLockManager;
    this.topicStore = topicStore;
    this.rqueueRedisMessagePublisher = rqueueRedisMessagePublisher;
    this.rqueueMessageConverter = rqueueMessageConverter;
    this.rqueueMessageTemplate = rqueueMessageTemplate;
  }

  private void validateTopicCreation(List<Topic> topics) throws ValidationException {
    Set<Topic> registeredTopics = topicStore.getTopics();
    for (Topic topic : topics) {
      if (registeredTopics.contains(topic)) {
        throw new ValidationException(ErrorCode.TOPIC_ALREADY_EXIST);
      }
    }
    if (new HashSet<>(topics).size() != topics.size()) {
      throw new ValidationException(ErrorCode.DUPLICATE_TOPIC);
    }
  }

  @Override
  public CreateTopicResponse create(CreateTopicRequest request)
      throws LockException, ValidationException, ProcessingException {
    List<Topic> topics = request.getTopics();
    if (CollectionUtils.isEmpty(topics)) {
      throw new ValidationException(ErrorCode.TOPIC_LIST_CAN_NOT_BE_EMPTY);
    }
    Duration duration = Duration.ofSeconds(5);
    String topicKey = rqueueConfig.getTopicsKey();
    if (rqueueLockManager.acquireLock(topicKey, duration)) {
      validateTopicCreation(topics);
      topicStore.addTopics(topics);
      rqueueRedisMessagePublisher.publishBrokerTopic(EventType.ADD, topics);
      rqueueLockManager.releaseLock(topicKey);
      CreateTopicResponse response = new CreateTopicResponse();
      response.set(ErrorCode.SUCCESS, "topic created");
      return response;
    }
    throw new LockException("topic lock is not available");
  }

  @Override
  public SubscriptionResponse subscribe(SubscriptionRequest request)
      throws ValidationException, ProcessingException, LockException {
    Duration duration = Duration.ofSeconds(5);
    String topicsKey = rqueueConfig.getTopicsKey();
    if (rqueueLockManager.acquireLock(topicsKey, duration)) {
      Topic topic = request.getTopic();
      if (topicStore.isExist(topic)) {
        SubscriptionResponse response = new SubscriptionResponse();
        String topicKey = rqueueConfig.getTopicName(topic.getName());
        if (rqueueLockManager.acquireLock(topicKey, duration)) {
          validateSubscription(topic, request.getSubscriptions());
          topicStore.addSubscriptions(request.getTopic(), request.getSubscriptions());
          rqueueLockManager.releaseLock(topicKey);
          rqueueLockManager.releaseLock(topicsKey);
          rqueueRedisMessagePublisher.publishBrokerTopic(EventType.UPDATE, topic);
          response.set(ErrorCode.SUCCESS, "topic subscribed");
          return response;
        }
        rqueueLockManager.releaseLock(topicsKey);
        throw new LockException("topic lock is not available");
      }
      rqueueLockManager.releaseLock(topicsKey);
      throw new ValidationException(ErrorCode.TOPIC_DOES_NOT_EXIST);
    }
    throw new LockException("topic lock is not available");
  }

  private void validateSubscription(Topic topic, List<Subscription> destinations)
      throws ValidationException {
    List<Subscription> subscriptions = topicStore.getSubscriptions(topic);
    if (subscriptions.isEmpty()) {
      return;
    }
    for (Subscription subscription : destinations) {
      if (subscriptions.contains(subscription)) {
        throw new ValidationException(ErrorCode.DUPLICATE_SUBSCRIPTION);
      }
    }
  }

  @Override
  public UnsubscriptionResponse unsubscribe(UnsubscriptionRequest request)
      throws ValidationException, LockException, ProcessingException {
    Duration duration = Duration.ofSeconds(5);
    String topicsKey = rqueueConfig.getTopicsKey();
    if (rqueueLockManager.acquireLock(topicsKey, duration)) {
      Topic topic = request.getTopic();
      if (topicStore.isExist(topic)) {
        String topicKey = rqueueConfig.getTopicName(topic.getName());
        if (rqueueLockManager.acquireLock(topicKey, duration)) {
          List<SubscriptionConfig> subscriptionConfigs = topicStore.getSubscriptionConfig(topic);
          Observer target = request.getObserver();
          for (SubscriptionConfig subscriptionConfig : subscriptionConfigs) {
            if (subscriptionConfig.getTarget().equals(target.getTarget())
                && subscriptionConfig.getTargetType() == target.getTargetType()) {
              topicStore.removeSubscription(topic, subscriptionConfig);
              rqueueLockManager.releaseLock(topicKey);
              rqueueLockManager.releaseLock(topicsKey);
              rqueueRedisMessagePublisher.publishBrokerTopic(EventType.UPDATE, topic);
              UnsubscriptionResponse response = new UnsubscriptionResponse();
              response.set(ErrorCode.SUCCESS, "Unsubscribed");
              return response;
            }
          }
          rqueueLockManager.releaseLock(topicKey);
          rqueueLockManager.releaseLock(topicsKey);
          throw new ValidationException(ErrorCode.SUBSCRIPTION_DOES_NOT_EXIST);
        }
        rqueueLockManager.releaseLock(topicsKey);
        throw new LockException("topic lock is not available");
      }
      rqueueLockManager.releaseLock(topicsKey);
      throw new ValidationException(ErrorCode.TOPIC_DOES_NOT_EXIST);
    }
    throw new LockException("topic lock is not available");
  }

  private RqueueMessage createMessage(MessagePushRequest request) throws ProcessingException {
    String msg;
    msg = rqueueMessageConverter.fromMessage(request.getMessage());
    return new RqueueMessage(request.getTopic().getName(), msg, null, null);
  }

  @Override
  public MessagePublishResponse publish(MessagePublishRequest request)
      throws ValidationException, ProcessingException {
    List<MessagePushRequest> messagePushRequests = request.getMessages();
    if (CollectionUtils.isEmpty(messagePushRequests)) {
      throw new ValidationException(ErrorCode.NO_MESSAGE_PROVIDED);
    }
    List<IdResponse> responses = new ArrayList<>();
    for (MessagePushRequest messagePushRequest : messagePushRequests) {
      Topic topic = messagePushRequest.getTopic();
      if (topicStore.isExist(topic)) {
        String topicKey = rqueueConfig.getTopicName(messagePushRequest.getTopic().getName());
        RqueueMessage message = createMessage(messagePushRequest);
        rqueueMessageTemplate.addMessage(topicKey, message);
        responses.add(new IdResponse(message.getId()));
      } else {
        responses.add(new IdResponse(ErrorCode.TOPIC_DOES_NOT_EXIST));
      }
    }
    return new MessagePublishResponse(responses);
  }

  @Override
  public SubscriptionUpdateResponse updateSubscription(SubscriptionUpdateRequest request)
      throws ValidationException, ProcessingException, LockException {
    Duration duration = Duration.ofSeconds(5);
    String topicsKey = rqueueConfig.getTopicsKey();
    if (rqueueLockManager.acquireLock(topicsKey, duration)) {
      Topic topic = request.getTopic();
      if (topicStore.isExist(topic)) {
        SubscriptionUpdateResponse response = new SubscriptionUpdateResponse();
        String topicKey = rqueueConfig.getTopicName(topic.getName());
        if (rqueueLockManager.acquireLock(topicKey, duration)) {
          List<SubscriptionConfig> subscriptionConfigs = topicStore.getSubscriptionConfig(topic);
          Subscription subscription = request.getSubscription();
          for (SubscriptionConfig subscriptionConfig : subscriptionConfigs) {
            if (subscriptionConfig.getTargetType() == subscription.getTargetType()
                && subscription.getTarget().equals(subscriptionConfig.getTarget())) {
              topicStore.updateSubscription(topic, subscriptionConfig, subscription);
              rqueueLockManager.releaseLock(topicKey);
              rqueueLockManager.releaseLock(topicsKey);
              rqueueRedisMessagePublisher.publishBrokerTopic(EventType.UPDATE, topic);
              response.set(ErrorCode.SUCCESS, "subscription updated");
              return response;
            }
          }
          rqueueLockManager.releaseLock(topicKey);
          rqueueLockManager.releaseLock(topicsKey);
          throw new ValidationException(ErrorCode.SUBSCRIPTION_DOES_NOT_EXIST);
        }
        rqueueLockManager.releaseLock(topicsKey);
        throw new LockException("topic lock is not available");
      }
      rqueueLockManager.releaseLock(topicsKey);
      throw new ValidationException(ErrorCode.TOPIC_DOES_NOT_EXIST);
    }
    throw new LockException("topic lock is not available");
  }

  @Override
  public DeleteTopicResponse delete(DeleteTopicRequest request)
      throws LockException, ProcessingException, ValidationException {
    Duration duration = Duration.ofSeconds(5);
    String topicsKey = rqueueConfig.getTopicsKey();
    if (rqueueLockManager.acquireLock(topicsKey, duration)) {
      TopicConfig topicConfig = topicStore.getTopicConfig(request.getTopic());
      if (topicConfig != null && !topicConfig.isDeleted()) {
        DeleteTopicResponse response = new DeleteTopicResponse();
        topicStore.removeTopic(request.getTopic());
        rqueueLockManager.releaseLock(topicsKey);
        rqueueRedisMessagePublisher.publishBrokerTopic(EventType.DELETION, request.getTopic());
        response.set(ErrorCode.SUCCESS, "topic deleted");
      }
      rqueueLockManager.releaseLock(topicsKey);
      throw new ValidationException(ErrorCode.TOPIC_DOES_NOT_EXIST);
    }
    throw new LockException("topic lock is not available");
  }
}
