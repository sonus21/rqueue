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

package com.github.sonus21.rqueue.core.impl;

import static com.github.sonus21.rqueue.utils.Validator.validateDelay;
import static com.github.sonus21.rqueue.utils.Validator.validateMessage;
import static com.github.sonus21.rqueue.utils.Validator.validateMessageId;
import static com.github.sonus21.rqueue.utils.Validator.validatePriority;
import static com.github.sonus21.rqueue.utils.Validator.validateQueue;
import static com.github.sonus21.rqueue.utils.Validator.validateRetryCount;

import com.github.sonus21.rqueue.core.RqueueMessageEnqueuer;
import com.github.sonus21.rqueue.core.RqueueMessageTemplate;
import com.github.sonus21.rqueue.utils.PriorityUtils;
import java.util.Collections;
import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.springframework.messaging.converter.MessageConverter;

@Slf4j
public class RqueueMessageEnqueuerImpl extends BaseMessageSender implements RqueueMessageEnqueuer {

  public RqueueMessageEnqueuerImpl(RqueueMessageTemplate messageTemplate) {
    super(messageTemplate, Collections.emptyList());
  }

  public RqueueMessageEnqueuerImpl(
      RqueueMessageTemplate messageTemplate, List<MessageConverter> messageConverters) {
    super(messageTemplate, messageConverters);
  }

  @Override
  public String enqueue(String queueName, Object message) {
    validateQueue(queueName);
    validateMessage(message);
    return pushMessage(queueName, null, message, null, null);
  }

  @Override
  public boolean enqueue(String queueName, String messageId, Object message) {
    validateQueue(queueName);
    validateMessageId(messageId);
    validateMessage(message);
    return pushMessage(queueName, messageId, message, null, null) != null;
  }

  @Override
  public String enqueueWithRetry(String queueName, Object message, int retryCount) {
    validateQueue(queueName);
    validateMessage(message);
    validateRetryCount(retryCount);
    return pushMessage(queueName, null, message, retryCount, null);
  }

  @Override
  public boolean enqueueWithRetry(
      String queueName, String messageId, Object message, int retryCount) {
    validateQueue(queueName);
    validateMessageId(messageId);
    validateMessage(message);
    validateRetryCount(retryCount);
    return pushMessage(queueName, messageId, message, retryCount, null) != null;
  }

  @Override
  public String enqueueWithPriority(String queueName, String priority, Object message) {
    validateQueue(queueName);
    validatePriority(priority);
    validateMessage(message);
    return pushMessage(
        PriorityUtils.getQueueNameForPriority(queueName, priority), null, message, null, null);
  }

  @Override
  public boolean enqueueWithPriority(
      String queueName, String priority, String messageId, Object message) {
    validateQueue(queueName);
    validatePriority(priority);
    validateMessageId(messageId);
    validateMessage(message);
    return pushMessage(
            PriorityUtils.getQueueNameForPriority(queueName, priority),
            messageId,
            message,
            null,
            null)
        != null;
  }

  @Override
  public String enqueueIn(String queueName, Object message, long delayInMilliSecs) {
    validateQueue(queueName);
    validateMessage(message);
    validateDelay(delayInMilliSecs);
    return pushMessage(queueName, null, message, null, delayInMilliSecs);
  }

  @Override
  public boolean enqueueIn(
      String queueName, String messageId, Object message, long delayInMilliSecs) {
    validateQueue(queueName);
    validateMessageId(messageId);
    validateMessage(message);
    validateDelay(delayInMilliSecs);
    return pushMessage(queueName, messageId, message, null, delayInMilliSecs) != null;
  }

  @Override
  public String enqueueInWithRetry(
      String queueName, Object message, int retryCount, long delayInMilliSecs) {
    validateQueue(queueName);
    validateMessage(message);
    validateRetryCount(retryCount);
    validateDelay(delayInMilliSecs);
    return pushMessage(queueName, null, message, retryCount, delayInMilliSecs);
  }

  @Override
  public boolean enqueueInWithRetry(
      String queueName, String messageId, Object message, int retryCount, long delayInMilliSecs) {
    validateQueue(queueName);
    validateMessageId(messageId);
    validateMessage(message);
    validateRetryCount(retryCount);
    validateDelay(delayInMilliSecs);
    return pushMessage(queueName, messageId, message, retryCount, delayInMilliSecs) != null;
  }

  @Override
  public String enqueueInWithPriority(
      String queueName, String priority, Object message, long delayInMilliSecs) {
    validateQueue(queueName);
    validatePriority(priority);
    validateMessage(message);
    validateDelay(delayInMilliSecs);
    return pushMessage(
        PriorityUtils.getQueueNameForPriority(queueName, priority),
        null,
        message,
        null,
        delayInMilliSecs);
  }

  @Override
  public boolean enqueueInWithPriority(
      String queueName, String priority, String messageId, Object message, long delayInMilliSecs) {
    validateQueue(queueName);
    validatePriority(priority);
    validateMessage(message);
    validateDelay(delayInMilliSecs);
    return pushMessage(
            PriorityUtils.getQueueNameForPriority(queueName, priority),
            messageId,
            message,
            null,
            delayInMilliSecs)
        != null;
  }
}
