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

package com.github.sonus21.rqueue.listener;

import static org.springframework.util.Assert.notEmpty;

import com.github.sonus21.rqueue.annotation.RqueueListener;
import com.github.sonus21.rqueue.converter.GenericMessageConverter;
import com.github.sonus21.rqueue.utils.QueueUtils;
import com.github.sonus21.rqueue.utils.ValueResolver;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.springframework.core.annotation.AnnotationUtils;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessagingException;
import org.springframework.messaging.converter.CompositeMessageConverter;
import org.springframework.messaging.converter.MessageConverter;
import org.springframework.messaging.handler.HandlerMethod;
import org.springframework.messaging.handler.annotation.support.AnnotationExceptionHandlerMethodResolver;
import org.springframework.messaging.handler.annotation.support.PayloadArgumentResolver;
import org.springframework.messaging.handler.invocation.AbstractExceptionHandlerMethodResolver;
import org.springframework.messaging.handler.invocation.AbstractMethodMessageHandler;
import org.springframework.messaging.handler.invocation.HandlerMethodArgumentResolver;
import org.springframework.messaging.handler.invocation.HandlerMethodReturnValueHandler;
import org.springframework.util.comparator.ComparableComparator;

public class RqueueMessageHandler extends AbstractMethodMessageHandler<MappingInformation> {
  private List<MessageConverter> messageConverters;

  public RqueueMessageHandler() {
    messageConverters = new ArrayList<>();
    addDefaultMessageConverter();
  }

  public RqueueMessageHandler(List<MessageConverter> messageConverters) {
    setMessageConverters(messageConverters);
  }

  private void addDefaultMessageConverter() {
    messageConverters.add(new GenericMessageConverter());
  }

  @Override
  protected List<? extends HandlerMethodArgumentResolver> initArgumentResolvers() {
    List<HandlerMethodArgumentResolver> resolvers = new ArrayList<>(getCustomArgumentResolvers());
    CompositeMessageConverter compositeMessageConverter =
        new CompositeMessageConverter(getMessageConverters());
    resolvers.add(new PayloadArgumentResolver(compositeMessageConverter));
    return resolvers;
  }

  @Override
  protected List<? extends HandlerMethodReturnValueHandler> initReturnValueHandlers() {
    return new ArrayList<>(getCustomReturnValueHandlers());
  }

  @Override
  protected boolean isHandler(Class<?> beanType) {
    return true;
  }

  @Override
  protected MappingInformation getMappingForMethod(Method method, Class<?> handlerType) {
    RqueueListener rqueueListener = AnnotationUtils.findAnnotation(method, RqueueListener.class);
    if (rqueueListener != null) {
      Set<String> queueNames = resolveQueueNames(rqueueListener);
      boolean isDelayedQueue = isDeadLetterQueue(rqueueListener);
      String deadLetterQueueName = resolveDelayedQueue(rqueueListener);
      int numRetries = resolveNumRetries(rqueueListener);
      long visibilityTimeout = resolveVisibilityTimeout(rqueueListener);
      boolean active = isActive(rqueueListener);
      MappingInformation mappingInformation =
          new MappingInformation(
              queueNames,
              isDelayedQueue,
              numRetries,
              deadLetterQueueName,
              visibilityTimeout,
              active);

      if (mappingInformation.isValid()) {
        return mappingInformation;
      }
      logger.warn("Queue '" + mappingInformation + "' not configured");
    }
    return null;
  }

  private long resolveVisibilityTimeout(RqueueListener rqueueListener) {
    return ValueResolver.resolveKeyToLong(
        getApplicationContext(), rqueueListener.visibilityTimeout());
  }

  private int resolveNumRetries(RqueueListener rqueueListener) {
    return ValueResolver.resolveKeyToInteger(getApplicationContext(), rqueueListener.numRetries());
  }

  private String resolveDelayedQueue(RqueueListener rqueueListener) {
    String dlqName = rqueueListener.deadLetterQueue();
    String[] resolvedValues =
        ValueResolver.resolveKeyToArrayOfStrings(getApplicationContext(), dlqName);
    if (resolvedValues.length == 1) {
      return resolvedValues[0];
    }
    throw new IllegalStateException(
        "more than one dead letter queue cannot be configured '" + dlqName + "'");
  }

  private boolean isDeadLetterQueue(RqueueListener rqueueListener) {
    return ValueResolver.resolveToBoolean(getApplicationContext(), rqueueListener.delayedQueue());
  }

  private boolean isActive(RqueueListener rqueueListener) {
    return ValueResolver.resolveToBoolean(getApplicationContext(), rqueueListener.active());
  }

  private Set<String> resolveQueueNames(RqueueListener rqueueListener) {
    String[] queueNames = rqueueListener.value();
    Set<String> result = new HashSet<>(queueNames.length);
    for (String queueName : queueNames) {
      result.addAll(
          Arrays.asList(
              ValueResolver.resolveKeyToArrayOfStrings(getApplicationContext(), queueName)));
    }
    return result;
  }

  @Override
  protected Set<String> getDirectLookupDestinations(MappingInformation mapping) {
    return mapping.getQueueNames();
  }

  @Override
  protected String getDestination(Message<?> message) {
    return (String) message.getHeaders().get(QueueUtils.getMessageHeaderKey());
  }

  @Override
  protected MappingInformation getMatchingMapping(MappingInformation mapping, Message<?> message) {
    if (mapping.getQueueNames().contains(getDestination(message))) {
      return mapping;
    }
    return null;
  }

  @Override
  protected Comparator<MappingInformation> getMappingComparator(Message<?> message) {
    return new ComparableComparator<>();
  }

  @Override
  protected AbstractExceptionHandlerMethodResolver createExceptionHandlerMethodResolverFor(
      Class<?> beanType) {
    return new AnnotationExceptionHandlerMethodResolver(beanType);
  }

  @Override
  protected void processHandlerMethodException(
      HandlerMethod handlerMethod, Exception ex, Message<?> message) {
    super.processHandlerMethodException(handlerMethod, ex, message);
    throw new MessagingException("An exception occurred while invoking the handler method", ex);
  }

  public List<MessageConverter> getMessageConverters() {
    return messageConverters;
  }

  public void setMessageConverters(List<MessageConverter> messageConverters) {
    notEmpty(messageConverters, "messageConverters list cannot be empty or null");
    this.messageConverters = messageConverters;
    addDefaultMessageConverter();
  }
}
