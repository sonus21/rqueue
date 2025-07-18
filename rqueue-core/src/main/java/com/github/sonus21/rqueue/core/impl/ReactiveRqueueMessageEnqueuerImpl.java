/*
 * Copyright (c) 2021-2025 Sonu Kumar
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

import static com.github.sonus21.rqueue.utils.Validator.*;

import com.github.sonus21.rqueue.core.EndpointRegistry;
import com.github.sonus21.rqueue.core.ReactiveRqueueMessageEnqueuer;
import com.github.sonus21.rqueue.core.RqueueMessage;
import com.github.sonus21.rqueue.core.RqueueMessageTemplate;
import com.github.sonus21.rqueue.core.support.RqueueMessageUtils;
import com.github.sonus21.rqueue.exception.DuplicateMessageException;
import com.github.sonus21.rqueue.listener.QueueDetail;
import com.github.sonus21.rqueue.utils.PriorityUtils;
import lombok.extern.slf4j.Slf4j;
import org.springframework.messaging.MessageHeaders;
import org.springframework.messaging.converter.MessageConverter;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.function.BiFunction;

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
      boolean isUnique,
      BiFunction<Long, RqueueMessage, Mono<T>> monoConverter) {
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
    try {
      Mono<Boolean> storeResult =
          (Mono<Boolean>) storeMessageMetadata(rqueueMessage, delayInMilliSecs, true, isUnique);
      return storeResult.flatMap(
          success -> {
            if (Boolean.TRUE.equals(success)) {
              Object result = enqueue(queueDetail, rqueueMessage, delayInMilliSecs, true);
              Mono<Long> enqueueMono;
              if (result instanceof Flux) {
                enqueueMono = ((Flux<Long>) result).next();
              } else if (result instanceof Mono) {
                enqueueMono = (Mono<Long>) result;
              } else {
                return Mono.error(
                    new IllegalStateException(
                        "Unexpected enqueue result type: " + result.getClass()));
              }
              return enqueueMono.flatMap(time -> monoConverter.apply(time, rqueueMessage));
            } else {
              return Mono.error(new DuplicateMessageException(rqueueMessage.getId()));
            }
          });
    } catch (Exception e) {
      log.error(
          "Failed to enqueue message [{}] to queue [{}]", rqueueMessage.getId(), queueName, e);
      return Mono.error(e);
    }
  }

  private void validateBasic(String queue, Object msg) {
    validateQueue(queue);
    validateMessage(msg);
  }

  private void validateWithId(String queue, String id, Object msg) {
    validateQueue(queue);
    validateMessageId(id);
    validateMessage(msg);
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
        false,
        (ignore, rqueueMessage) -> Mono.just(rqueueMessage.getId()));
  }

  private Mono<Boolean> pushReactiveWithMessageId(
      String queueName,
      String messageId,
      Object message,
      Integer retryCount,
      Long delayInMilliSecs,
      boolean isUnique) {
    return pushReactiveMessage(
        RqueueMessageUtils::buildMessage,
        queueName,
        messageId,
        message,
        retryCount,
        delayInMilliSecs,
        isUnique,
        (ignore, rqueueMessage) -> Mono.just(Boolean.TRUE));
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
        false,
        (ignore, rqueueMessage) -> Mono.just(rqueueMessage.getId()));
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
        false,
        (ignore, rqueueMessage) -> Mono.just(Boolean.TRUE));
  }

  @Override
  public Mono<String> enqueue(String queueName, Object message) {
    validateBasic(queueName, message);
    return pushReactiveMessage(queueName, message, null, null);
  }

  @Override
  public Mono<Boolean> enqueue(String queueName, String messageId, Object message) {
    validateWithId(queueName, messageId, message);
    return pushReactiveWithMessageId(queueName, messageId, message, null, null, false);
  }

  @Override
  public Mono<Boolean> enqueueUnique(String queueName, String messageId, Object message) {
    validateWithId(queueName, messageId, message);
    return pushReactiveWithMessageId(queueName, messageId, message, null, null, true);
  }

  @Override
  public Mono<String> enqueueWithRetry(String queueName, Object message, int retryCount) {
    validateBasic(queueName, message);
    validateRetryCount(retryCount);
    return pushReactiveMessage(queueName, message, retryCount, null);
  }

  @Override
  public Mono<Boolean> enqueueWithRetry(
      String queueName, String messageId, Object message, int retryCount) {
    validateWithId(queueName, messageId, message);
    validateRetryCount(retryCount);
    return pushReactiveWithMessageId(queueName, messageId, message, retryCount, null, false);
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
    validateWithId(queueName, messageId, message);
    validatePriority(priority);
    return pushReactiveWithMessageId(
        PriorityUtils.getQueueNameForPriority(queueName, priority),
        messageId,
        message,
        null,
        null,
        false);
  }

  @Override
  public Mono<String> enqueueIn(String queueName, Object message, long delayInMilliSecs) {
    validateBasic(queueName, message);
    validateDelay(delayInMilliSecs);
    return pushReactiveMessage(queueName, message, null, delayInMilliSecs);
  }

  @Override
  public Mono<Boolean> enqueueIn(
      String queueName, String messageId, Object message, long delayInMilliSecs) {
    validateWithId(queueName, messageId, message);
    validateDelay(delayInMilliSecs);
    return pushReactiveWithMessageId(queueName, messageId, message, null, delayInMilliSecs, false);
  }

  @Override
  public Mono<Boolean> enqueueUniqueIn(
      String queueName, String messageId, Object message, long delayInMillisecond) {
    validateWithId(queueName, messageId, message);
    validateDelay(delayInMillisecond);
    return pushReactiveWithMessageId(queueName, messageId, message, null, delayInMillisecond, true);
  }

  @Override
  public Mono<String> enqueueInWithRetry(
      String queueName, Object message, int retryCount, long delayInMilliSecs) {
    validateBasic(queueName, message);
    validateRetryCount(retryCount);
    validateDelay(delayInMilliSecs);
    return pushReactiveMessage(queueName, message, retryCount, delayInMilliSecs);
  }

  @Override
  public Mono<Boolean> enqueueInWithRetry(
      String queueName, String messageId, Object message, int retryCount, long delayInMilliSecs) {
    validateWithId(queueName, messageId, message);
    validateRetryCount(retryCount);
    validateDelay(delayInMilliSecs);
    return pushReactiveWithMessageId(
        queueName, messageId, message, retryCount, delayInMilliSecs, false);
  }

  @Override
  public Mono<String> enqueuePeriodic(String queueName, Object message, long periodInMilliSeconds) {
    validateBasic(queueName, message);
    validatePeriod(periodInMilliSeconds);
    return pushReactivePeriodicMessage(queueName, message, periodInMilliSeconds);
  }

  @Override
  public Mono<Boolean> enqueuePeriodic(
      String queueName, String messageId, Object message, long periodInMilliSeconds) {
    validateWithId(queueName, messageId, message);
    validatePeriod(periodInMilliSeconds);
    return pushReactivePeriodicMessageWithMessageId(
        queueName, messageId, message, periodInMilliSeconds);
  }

  @FunctionalInterface
  private interface MonoConverter<T> {
    T convert(Long id, Boolean success);
  }

  @FunctionalInterface
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
}
