/*
 * Copyright (c) 2021-2023 Sonu Kumar
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * You may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and limitations under the License.
 *
 */

package com.github.sonus21.rqueue.core.impl;

import static com.github.sonus21.rqueue.utils.Validator.validateDelay;
import static com.github.sonus21.rqueue.utils.Validator.validateMessage;
import static com.github.sonus21.rqueue.utils.Validator.validateMessageId;
import static com.github.sonus21.rqueue.utils.Validator.validatePeriod;
import static com.github.sonus21.rqueue.utils.Validator.validatePriority;
import static com.github.sonus21.rqueue.utils.Validator.validateQueue;
import static com.github.sonus21.rqueue.utils.Validator.validateRetryCount;

import com.github.sonus21.rqueue.core.EndpointRegistry;
import com.github.sonus21.rqueue.core.ReactiveRqueueMessageEnqueuer;
import com.github.sonus21.rqueue.core.RqueueMessage;
import com.github.sonus21.rqueue.core.RqueueMessageTemplate;
import com.github.sonus21.rqueue.core.support.RqueueMessageUtils;
import com.github.sonus21.rqueue.listener.QueueDetail;
import com.github.sonus21.rqueue.utils.PriorityUtils;
import lombok.extern.slf4j.Slf4j;
import org.springframework.messaging.MessageHeaders;
import org.springframework.messaging.converter.MessageConverter;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@Slf4j
public class ReactiveRqueueMessageEnqueuerImpl extends BaseMessageSender
    implements ReactiveRqueueMessageEnqueuer {

  public ReactiveRqueueMessageEnqueuerImpl(
      RqueueMessageTemplate messageTemplate,
      MessageConverter messageConverter,
      MessageHeaders messageHeaders) {
    super(messageTemplate, messageConverter, messageHeaders);
  }

  @SuppressWarnings("unchecked")
  private <T> Mono<T> pushReactiveMessage(
      MessageBuilder builder,
      String queueName,
      String messageId,
      Object message,
      Integer retryCount,
      Long delayInMilliSecs,
      MonoConverterGenerator<T> monoConverterGenerator) {
    QueueDetail queueDetail = EndpointRegistry.get(queueName);
    RqueueMessage rqueueMessage =
        builder.build(
            messageConverter,
            queueName,
            messageId,
            message,
            retryCount,
            delayInMilliSecs,
            messageHeaders);
    MonoConverter<T> monoConverter = monoConverterGenerator.create(rqueueMessage);
    try {
      Object o1 = enqueue(queueDetail, rqueueMessage, delayInMilliSecs, true);
      Mono<Boolean> storeMessageResult =
          (Mono<Boolean>) storeMessageMetadata(rqueueMessage, delayInMilliSecs, true);
      Mono<Long> longMono;
      if (o1 instanceof Flux) {
        longMono = ((Flux<Long>) o1).elementAt(0);
      } else {
        longMono = (Mono<Long>) o1;
      }
      return longMono.zipWith(storeMessageResult, monoConverter::call);
    } catch (Exception e) {
      log.error("Queue: {} Message {} could not be pushed {}", queueName, rqueueMessage, e);
      return Mono.error(e);
    }
  }

  private Mono<String> pushReactiveMessage(
      String queueName, Object message, Integer retryCount, Long delayInMilliSecs) {
    return pushReactiveMessage(
        RqueueMessageUtils::buildMessage,
        queueName,
        null,
        message,
        retryCount,
        delayInMilliSecs,
        new StrMonoConverterGenerator());
  }

  private Mono<Boolean> pushReactiveWithMessageId(
      String queueName,
      String messageId,
      Object message,
      Integer retryCount,
      Long delayInMilliSecs) {
    return pushReactiveMessage(
        RqueueMessageUtils::buildMessage,
        queueName,
        messageId,
        message,
        retryCount,
        delayInMilliSecs,
        new BooleanMonoConverterGenerator());
  }

  private Mono<String> pushReactivePeriodicMessage(
      String queueName, Object message, long periodInMilliSeconds) {
    return pushReactiveMessage(
        RqueueMessageUtils::buildPeriodicMessage,
        queueName,
        null,
        message,
        null,
        periodInMilliSeconds,
        new StrMonoConverterGenerator());
  }

  private Mono<Boolean> pushReactivePeriodicMessageWithMessageId(
      String queueName, String messageId, Object message, long periodInMilliSeconds) {
    return pushReactiveMessage(
        RqueueMessageUtils::buildPeriodicMessage,
        queueName,
        messageId,
        message,
        null,
        periodInMilliSeconds,
        new BooleanMonoConverterGenerator());
  }

  @Override
  public Mono<String> enqueue(String queueName, Object message) {
    validateQueue(queueName);
    validateMessage(message);
    return pushReactiveMessage(queueName, message, null, null);
  }

  @Override
  public Mono<Boolean> enqueue(String queueName, String messageId, Object message) {
    validateQueue(queueName);
    validateMessageId(messageId);
    validateMessage(message);
    return pushReactiveWithMessageId(queueName, messageId, message, null, null);
  }

  @Override
  public Mono<Boolean> enqueueUnique(String queueName, String messageId, Object message) {
    validateQueue(queueName);
    validateMessageId(messageId);
    validateMessage(message);
    // TODO uniqueness
    return pushReactiveWithMessageId(queueName, messageId, message, null, null);
  }

  @Override
  public Mono<String> enqueueWithRetry(String queueName, Object message, int retryCount) {
    validateQueue(queueName);
    validateMessage(message);
    validateRetryCount(retryCount);
    return pushReactiveMessage(queueName, message, retryCount, null);
  }

  @Override
  public Mono<Boolean> enqueueWithRetry(
      String queueName, String messageId, Object message, int retryCount) {
    validateQueue(queueName);
    validateMessageId(messageId);
    validateMessage(message);
    validateRetryCount(retryCount);
    return pushReactiveWithMessageId(queueName, messageId, message, retryCount, null);
  }

  @Override
  public Mono<String> enqueueWithPriority(String queueName, String priority, Object message) {
    validateQueue(queueName);
    validatePriority(priority);
    validateMessage(message);
    return pushReactiveMessage(
        PriorityUtils.getQueueNameForPriority(queueName, priority), message, null, null);
  }

  @Override
  public Mono<Boolean> enqueueWithPriority(
      String queueName, String priority, String messageId, Object message) {
    validateQueue(queueName);
    validatePriority(priority);
    validateMessageId(messageId);
    validateMessage(message);
    return pushReactiveWithMessageId(
        PriorityUtils.getQueueNameForPriority(queueName, priority), messageId, message, null, null);
  }

  @Override
  public Mono<String> enqueueIn(String queueName, Object message, long delayInMilliSecs) {
    validateQueue(queueName);
    validateMessage(message);
    validateDelay(delayInMilliSecs);
    return pushReactiveMessage(queueName, message, null, delayInMilliSecs);
  }

  @Override
  public Mono<Boolean> enqueueIn(
      String queueName, String messageId, Object message, long delayInMilliSecs) {
    validateQueue(queueName);
    validateMessageId(messageId);
    validateMessage(message);
    validateDelay(delayInMilliSecs);
    return pushReactiveWithMessageId(queueName, messageId, message, null, delayInMilliSecs);
  }

  @Override
  public Mono<Boolean> enqueueUniqueIn(
      String queueName, String messageId, Object message, long delayInMillisecond) {
    validateQueue(queueName);
    validateMessageId(messageId);
    validateMessage(message);
    validateDelay(delayInMillisecond);
    // TODO unique??
    return pushReactiveWithMessageId(queueName, messageId, message, null, delayInMillisecond);
  }

  @Override
  public Mono<String> enqueueInWithRetry(
      String queueName, Object message, int retryCount, long delayInMilliSecs) {
    validateQueue(queueName);
    validateMessage(message);
    validateRetryCount(retryCount);
    validateDelay(delayInMilliSecs);
    return pushReactiveMessage(queueName, message, retryCount, delayInMilliSecs);
  }

  @Override
  public Mono<Boolean> enqueueInWithRetry(
      String queueName, String messageId, Object message, int retryCount, long delayInMilliSecs) {
    validateQueue(queueName);
    validateMessageId(messageId);
    validateMessage(message);
    validateDelay(retryCount);
    validateDelay(delayInMilliSecs);
    return pushReactiveWithMessageId(queueName, messageId, message, retryCount, delayInMilliSecs);
  }

  @Override
  public Mono<String> enqueuePeriodic(String queueName, Object message, long periodInMilliSeconds) {
    validateQueue(queueName);
    validateMessage(message);
    validatePeriod(periodInMilliSeconds);
    return pushReactivePeriodicMessage(queueName, message, periodInMilliSeconds);
  }

  @Override
  public Mono<Boolean> enqueuePeriodic(
      String queueName, String messageId, Object message, long periodInMilliSeconds) {
    validateQueue(queueName);
    validateMessage(message);
    validateMessageId(messageId);
    validatePeriod(periodInMilliSeconds);
    return pushReactivePeriodicMessageWithMessageId(
        queueName, messageId, message, periodInMilliSeconds);
  }

  private interface MonoConverter<T> {

    T call(Long a, Boolean b);
  }

  private interface MessageBuilder {

    RqueueMessage build(
        MessageConverter converter,
        String queueName,
        String messageId,
        Object message,
        Integer retryCount,
        Long delay,
        MessageHeaders messageHeaders);
  }

  private interface MonoConverterGenerator<T> {

    MonoConverter<T> create(RqueueMessage rqueueMessage);
  }

  private static class StrMonoConverter implements MonoConverter<String> {

    private final RqueueMessage message;

    private StrMonoConverter(RqueueMessage message) {
      this.message = message;
    }

    @Override
    public String call(Long a, Boolean b) {
      return message.getId();
    }
  }

  private static class BoolMonoConverter implements MonoConverter<Boolean> {

    private BoolMonoConverter(RqueueMessage message) {
    }

    @Override
    public Boolean call(Long a, Boolean b) {
      return Boolean.TRUE;
    }
  }

  private static class StrMonoConverterGenerator implements MonoConverterGenerator<String> {

    @Override
    public MonoConverter<String> create(RqueueMessage rqueueMessage) {
      return new StrMonoConverter(rqueueMessage);
    }
  }

  private static class BooleanMonoConverterGenerator implements MonoConverterGenerator<Boolean> {

    @Override
    public MonoConverter<Boolean> create(RqueueMessage rqueueMessage) {
      return new BoolMonoConverter(rqueueMessage);
    }
  }
}
