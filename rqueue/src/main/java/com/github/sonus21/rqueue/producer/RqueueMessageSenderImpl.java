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

package com.github.sonus21.rqueue.producer;

import static org.springframework.util.Assert.isTrue;
import static org.springframework.util.Assert.notEmpty;
import static org.springframework.util.Assert.notNull;

import com.github.sonus21.rqueue.annotation.RqueueListener;
import com.github.sonus21.rqueue.common.RqueueRedisTemplate;
import com.github.sonus21.rqueue.converter.GenericMessageConverter;
import com.github.sonus21.rqueue.core.RqueueMessage;
import com.github.sonus21.rqueue.core.RqueueMessageTemplate;
import com.github.sonus21.rqueue.models.MessageMoveResult;
import com.github.sonus21.rqueue.utils.Constants;
import com.github.sonus21.rqueue.utils.QueueUtils;
import com.github.sonus21.rqueue.utils.Validator;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.messaging.converter.MessageConverter;
import org.springframework.messaging.converter.StringMessageConverter;

public class RqueueMessageSenderImpl implements RqueueMessageSender {
  private MessageWriter messageWriter;
  private RqueueMessageTemplate messageTemplate;
  @Autowired private RqueueRedisTemplate<String> stringRqueueRedisTemplate;

  private RqueueMessageSenderImpl(
      RqueueMessageTemplate messageTemplate,
      List<MessageConverter> messageConverters,
      boolean addDefault) {
    notNull(messageTemplate, "messageTemplate cannot be null");
    notEmpty(messageConverters, "messageConverters cannot be empty");
    this.messageTemplate = messageTemplate;
    messageWriter =
        new MessageWriter(messageTemplate, getMessageConverters(addDefault, messageConverters));
  }

  public RqueueMessageSenderImpl(RqueueMessageTemplate messageTemplate) {
    this(messageTemplate, Collections.singletonList(new GenericMessageConverter()), false);
  }

  public RqueueMessageSenderImpl(
      RqueueMessageTemplate messageTemplate, List<MessageConverter> messageConverters) {
    this(messageTemplate, messageConverters, true);
  }

  private List<MessageConverter> getMessageConverters(
      boolean addDefault, List<MessageConverter> messageConverters) {
    List<MessageConverter> messageConverterList = new ArrayList<>();
    StringMessageConverter stringMessageConverter = new StringMessageConverter();
    stringMessageConverter.setSerializedPayloadClass(String.class);
    messageConverterList.add(stringMessageConverter);
    if (addDefault) {
      messageConverterList.add(new GenericMessageConverter());
    }
    messageConverterList.addAll(messageConverters);
    return messageConverterList;
  }

  /**
   * Submit a message on given queue without any delay, listener would try to consume this message
   * immediately but due to heavy load message consumption can be delayed if message producer rate
   * is higher than the rate at consumer consume the messages.
   *
   * @param queueName on which queue message has to be send
   * @param message message object it could be any arbitrary object.
   * @return message was submitted successfully or failed.
   */
  @Override
  public boolean put(String queueName, Object message) {
    return enqueue(queueName, message);
  }

  @Override
  public boolean enqueue(String queueName, Object message) {
    Validator.validateQueueNameAndMessage(queueName, message);
    return messageWriter.pushMessage(queueName, message, null, null);
  }

  /**
   * This is an extension to the method {@link #put(String, Object)}. By default container would try
   * to deliver the same message for {@link Integer#MAX_VALUE} times, but that can be either
   * overridden using {@link RqueueListener#numRetries()}, even that value can be overridden using
   * this method.
   *
   * @param queueName on which queue message has to be send
   * @param message message object it could be any arbitrary object.
   * @param retryCount how many times a message would be retried, before it can be discarded or sent
   *     to dead letter queue configured using {@link RqueueListener#delayedQueue()}
   * @return message was submitted successfully or failed.
   */
  @Override
  public boolean put(String queueName, Object message, int retryCount) {
    return enqueue(queueName, message, retryCount);
  }

  /**
   * This is an extension to the method {@link #put(String, Object)}. By default container would try
   * to deliver the same message for {@link Integer#MAX_VALUE} times, but that can be either
   * overridden using {@link RqueueListener#numRetries()}, even that value can be overridden using
   * this method.
   *
   * @param queueName on which queue message has to be send
   * @param message message object it could be any arbitrary object.
   * @param retryCount how many times a message would be retried, before it can be discarded or sent
   *     to dead letter queue configured using {@link RqueueListener#delayedQueue()}
   * @return message was submitted successfully or failed.
   */
  @Override
  public boolean enqueue(String queueName, Object message, int retryCount) {
    Validator.validateQueueNameAndMessage(queueName, message);
    Validator.validateRetryCount(retryCount);
    return messageWriter.pushMessage(queueName, message, retryCount, null);
  }

  /**
   * This is the extension to the method {@link #put(String, Object)}, in this we can specify when
   * this message would be visible to the consumer.
   *
   * @param queueName on which queue message has to be send
   * @param message message object it could be any arbitrary object.
   * @param delayInMilliSecs delay in milli seconds, this message would be only visible to the
   *     listener when number of millisecond has elapsed.
   * @return message was submitted successfully or failed.
   */
  @Override
  public boolean put(String queueName, Object message, long delayInMilliSecs) {
    return enqueueIn(queueName, message, delayInMilliSecs);
  }

  /**
   * This is the extension to the method {@link #put(String, Object)}, in this we can specify when
   * this message would be visible to the consumer.
   *
   * @param queueName on which queue message has to be send
   * @param message message object it could be any arbitrary object.
   * @param delayInMilliSecs delay in milli seconds, this message would be only visible to the
   *     listener when number of millisecond has elapsed.
   * @return message was submitted successfully or failed.
   */
  @Override
  public boolean enqueueIn(String queueName, Object message, long delayInMilliSecs) {
    Validator.validateQueueNameAndMessage(queueName, message);
    Validator.validateDelay(delayInMilliSecs);
    return messageWriter.pushMessage(queueName, message, null, delayInMilliSecs);
  }

  /**
   * Find all messages stored on a given queue, it considers all the messages including delayed and
   * non-delayed.
   *
   * @param queueName queue name to be query for
   * @return list of messages.
   */
  @Override
  public List<Object> getAllMessages(String queueName) {
    List<Object> messages = new ArrayList<>();
    for (RqueueMessage message : messageTemplate.getAllMessages(queueName)) {
      messages.add(messageWriter.convertMessageToObject(message));
    }
    return messages;
  }

  /**
   * Extension to the {@link #put(String, Object, long)}, as like other retry method we can override
   * number of times a listener would be retrying
   *
   * @param queueName on which queue message has to be send
   * @param message message object it could be any arbitrary object.
   * @param retryCount how many times a message would be retried, before it can be discarded or sent
   *     to dead letter queue configured using {@link RqueueListener#delayedQueue()}
   * @param delayInMilliSecs delay in milli seconds, this message would be only visible to the
   *     listener when number of millisecond has elapsed.
   * @return message was submitted successfully or failed.
   */
  @Override
  public boolean put(String queueName, Object message, int retryCount, long delayInMilliSecs) {
    return enqueueIn(queueName, message, retryCount, delayInMilliSecs);
  }

  @Override
  public boolean enqueueIn(
      String queueName, Object message, int retryCount, long delayInMilliSecs) {
    Validator.validateQueueNameAndMessage(queueName, message);
    Validator.validateRetryCount(retryCount);
    Validator.validateDelay(delayInMilliSecs);
    return messageWriter.pushMessage(queueName, message, retryCount, delayInMilliSecs);
  }

  @Override
  public List<MessageConverter> getMessageConverters() {
    return messageWriter.getMessageConverters();
  }

  /**
   * Move messages from Dead Letter queue to the destination queue. This push the messages at the
   * FRONT of destination queue, so that it can be reprocessed as soon as possible.
   *
   * @param deadLetterQueueName dead letter queue name
   * @param queueName queue name
   * @param maxMessages number of messages to be moved by default move {@link
   *     Constants#MAX_MESSAGES} messages
   * @return success or failure.
   */
  @Override
  public boolean moveMessageFromDeadLetterToQueue(
      String deadLetterQueueName, String queueName, Integer maxMessages) {
    return moveMessageListToList(deadLetterQueueName, queueName, maxMessages).isSuccess();
  }

  /**
   * A shortcut to the method {@link #moveMessageFromDeadLetterToQueue(String, String, Integer)}
   *
   * @param deadLetterQueueName dead letter queue name
   * @param queueName queue name
   * @return success or failure
   */
  @Override
  public boolean moveMessageFromDeadLetterToQueue(String deadLetterQueueName, String queueName) {
    return moveMessageListToList(deadLetterQueueName, queueName, null).isSuccess();
  }

  /**
   * A shortcut to the method {@link #moveMessageFromDeadLetterToQueue(String, String, Integer)}
   *
   * @param sourceQueue source queue name
   * @param destinationQueue destination queue name
   * @return success or failure.
   * @see RqueueMessageTemplate
   */
  private MessageMoveResult moveMessageListToList(
      String sourceQueue, String destinationQueue, Integer maxMessage) {
    notNull(sourceQueue, "sourceQueue must not be null");
    notNull(destinationQueue, "destinationQueue must not be null");
    isTrue(
        !sourceQueue.equals(destinationQueue),
        "sourceQueue and destinationQueue must be different");
    Integer messageCount = maxMessage;
    if (messageCount == null) {
      messageCount = Constants.MAX_MESSAGES;
    }
    isTrue(messageCount > 0, "maxMessage must be greater than zero");
    return messageTemplate.moveMessageListToList(sourceQueue, destinationQueue, messageCount);
  }

  /**
   * Very dangerous method it will delete all messages in a queue
   *
   * @param queueName queue name
   */
  @Override
  public boolean deleteAllMessages(String queueName) {
    stringRqueueRedisTemplate.delete(queueName);
    stringRqueueRedisTemplate.delete(QueueUtils.getProcessingQueueName(queueName));
    stringRqueueRedisTemplate.delete(QueueUtils.getDelayedQueueName(queueName));
    return true;
  }
}
