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

import static org.springframework.util.Assert.notNull;

import com.github.sonus21.rqueue.annotation.RqueueListener;
import com.github.sonus21.rqueue.converter.GenericMessageConverter;
import com.github.sonus21.rqueue.core.EndpointRegistry;
import com.github.sonus21.rqueue.exception.QueueDoesNotExist;
import com.github.sonus21.rqueue.models.Concurrency;
import com.github.sonus21.rqueue.utils.Constants;
import com.github.sonus21.rqueue.utils.PriorityUtils;
import com.github.sonus21.rqueue.utils.ValueResolver;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.core.annotation.AnnotationUtils;
import org.springframework.core.convert.ConversionService;
import org.springframework.format.support.DefaultFormattingConversionService;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessagingException;
import org.springframework.messaging.converter.CompositeMessageConverter;
import org.springframework.messaging.converter.MessageConverter;
import org.springframework.messaging.handler.HandlerMethod;
import org.springframework.messaging.handler.annotation.support.AnnotationExceptionHandlerMethodResolver;
import org.springframework.messaging.handler.annotation.support.HeaderMethodArgumentResolver;
import org.springframework.messaging.handler.annotation.support.HeadersMethodArgumentResolver;
import org.springframework.messaging.handler.annotation.support.MessageMethodArgumentResolver;
import org.springframework.messaging.handler.annotation.support.PayloadArgumentResolver;
import org.springframework.messaging.handler.invocation.AbstractExceptionHandlerMethodResolver;
import org.springframework.messaging.handler.invocation.AbstractMethodMessageHandler;
import org.springframework.messaging.handler.invocation.HandlerMethodArgumentResolver;
import org.springframework.messaging.handler.invocation.HandlerMethodReturnValueHandler;
import org.springframework.messaging.simp.annotation.support.PrincipalMethodArgumentResolver;
import org.springframework.util.comparator.ComparableComparator;

public class RqueueMessageHandler extends AbstractMethodMessageHandler<MappingInformation> {
  private ConversionService conversionService = new DefaultFormattingConversionService();
  private final MessageConverter messageConverter;

  public RqueueMessageHandler() {
    this(Collections.emptyList());
  }

  public RqueueMessageHandler(List<MessageConverter> messageConverters) {
    notNull(messageConverters, "messageConverters cannot be null");
    List<MessageConverter> messageConverterList = new ArrayList<>(messageConverters);
    messageConverterList.add(new GenericMessageConverter());
    this.messageConverter = new CompositeMessageConverter(messageConverterList);
  }

  private ConfigurableBeanFactory getBeanFactory() {
    ApplicationContext context = getApplicationContext();
    return (context instanceof ConfigurableApplicationContext
        ? ((ConfigurableApplicationContext) context).getBeanFactory()
        : null);
  }

  @Override
  protected List<? extends HandlerMethodArgumentResolver> initArgumentResolvers() {
    List<HandlerMethodArgumentResolver> resolvers = new ArrayList<>(getCustomArgumentResolvers());
    // Annotation-based argument resolution
    resolvers.add(new HeaderMethodArgumentResolver(this.conversionService, getBeanFactory()));
    resolvers.add(new HeadersMethodArgumentResolver());

    // Type-based argument resolution
    resolvers.add(new PrincipalMethodArgumentResolver());
    resolvers.add(new MessageMethodArgumentResolver(messageConverter));
    resolvers.add(new PayloadArgumentResolver(messageConverter, null));

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

  private Concurrency resolveConcurrency(RqueueListener rqueueListener) {
    String val =
        ValueResolver.resolveKeyToString(getApplicationContext(), rqueueListener.concurrency());
    if (val.equals("-1")) {
      return new Concurrency(-1, -1);
    }
    String[] vals = val.split("-");
    if (vals.length > 2 || vals.length == 0) {
      throw new IllegalStateException(
          "Concurrency must be either some number e.g. 5 or in the form of 5-10");
    }
    if (vals.length == 1) {
      int concurrency =
          parseInt(vals[0], "Concurrency is not a number", "Concurrency is not a number");
      return new Concurrency(1, concurrency);
    }
    int lowerLimit =
        parseInt(
            vals[0],
            "Concurrency lower limit is not a number",
            "Concurrency lower limit must be non-zero");
    int upperLimit =
        parseInt(
            vals[1],
            "Concurrency upper limit is not a number",
            "Concurrency upper limit must be non-zero");
    if (lowerLimit > upperLimit) {
      throw new IllegalStateException("upper limit of concurrency is smaller than the lower limit");
    }
    return new Concurrency(lowerLimit, upperLimit);
  }

  private String resolvePriorityGroup(RqueueListener rqueueListener) {
    return ValueResolver.resolveKeyToString(
        getApplicationContext(), rqueueListener.priorityGroup());
  }

  private int parseInt(String txt, String message, String nonZeroText) {
    try {
      int n = Integer.parseInt(txt);
      if (n <= 0) {
        throw new IllegalStateException(nonZeroText);
      }
      return n;
    } catch (NumberFormatException e) {
      throw new IllegalStateException(message, e);
    }
  }

  private Map<String, Integer> resolvePriority(RqueueListener rqueueListener) {
    String[] priorities =
        ValueResolver.resolveKeyToArrayOfStrings(
            getApplicationContext(), rqueueListener.priority());
    HashMap<String, Integer> priorityMap = new HashMap<>();
    if (priorities.length == 0 || (priorities[0].equals(Constants.BLANK))) {
      return priorityMap;
    }
    for (String priority : priorities) {
      String[] vals = priority.split(":");
      if (vals.length == 1) {
        vals = priority.split("=");
      }
      if (vals.length == 1) {
        if (!priorityMap.isEmpty()) {
          throw new IllegalStateException("Invalid priority configuration is used.");
        }
        priorityMap.put(
            Constants.DEFAULT_PRIORITY_KEY,
            parseInt(
                vals[0],
                "priority is not a number.",
                "priority must be greater than or equal to 1"));
      } else if (vals.length == 2) {
        priorityMap.put(
            vals[0],
            parseInt(
                vals[1],
                "priority is not a number.",
                "priority must be greater than or equal to 1"));
      } else {
        throw new IllegalStateException("Priority cannot be parsed");
      }
    }
    return Collections.unmodifiableMap(priorityMap);
  }

  @Override
  protected MappingInformation getMappingForMethod(Method method, Class<?> handlerType) {
    RqueueListener rqueueListener = AnnotationUtils.findAnnotation(method, RqueueListener.class);
    if (rqueueListener != null) {
      Set<String> queueNames = resolveQueueNames(rqueueListener);
      String deadLetterQueueName = resolveDeadLetterQueue(rqueueListener);
      int numRetries = resolveNumRetries(rqueueListener);
      long visibilityTimeout = resolveVisibilityTimeout(rqueueListener);
      boolean active = isActive(rqueueListener);
      boolean consumerEnabled = resolveConsumerEnabled(rqueueListener);
      Concurrency concurrency = resolveConcurrency(rqueueListener);
      Map<String, Integer> priorityMap = resolvePriority(rqueueListener);
      String priorityGroup = resolvePriorityGroup(rqueueListener);
      MappingInformation mappingInformation =
          MappingInformation.builder()
              .active(active)
              .concurrency(concurrency)
              .deadLetterQueueName(deadLetterQueueName)
              .deadLetterConsumerEnabled(consumerEnabled)
              .numRetry(numRetries)
              .queueNames(queueNames)
              .visibilityTimeout(visibilityTimeout)
              .priorityGroup(priorityGroup)
              .priority(priorityMap)
              .build();
      if (mappingInformation.isValid()) {
        return mappingInformation;
      }
      logger.warn("Queue '" + mappingInformation + "' not configured");
    }
    return null;
  }

  private boolean resolveConsumerEnabled(RqueueListener rqueueListener) {
    return ValueResolver.resolveToBoolean(
        getApplicationContext(), rqueueListener.deadLetterQueueListenerEnabled());
  }

  private long resolveVisibilityTimeout(RqueueListener rqueueListener) {
    return ValueResolver.resolveKeyToLong(
        getApplicationContext(), rqueueListener.visibilityTimeout());
  }

  private int resolveNumRetries(RqueueListener rqueueListener) {
    return ValueResolver.resolveKeyToInteger(getApplicationContext(), rqueueListener.numRetries());
  }

  private String resolveDeadLetterQueue(RqueueListener rqueueListener) {
    String dlqName = rqueueListener.deadLetterQueue();
    String[] resolvedValues =
        ValueResolver.resolveKeyToArrayOfStrings(getApplicationContext(), dlqName);
    if (resolvedValues.length == 1) {
      return resolvedValues[0];
    }
    throw new IllegalStateException(
        "more than one dead letter queue cannot be configured '" + dlqName + "'");
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
    return Collections.unmodifiableSet(result);
  }

  @Override
  protected Set<String> getDirectLookupDestinations(MappingInformation mapping) {
    Set<String> destinations = new HashSet<>(mapping.getQueueNames());
    for (String queueName : mapping.getQueueNames()) {
      destinations.addAll(PriorityUtils.getNamesFromPriority(queueName, mapping.getPriority()));
    }
    return destinations;
  }

  @Override
  protected String getDestination(Message<?> message) {
    return (String) message.getHeaders().get(RqueueMessageHeaders.DESTINATION);
  }

  @Override
  protected MappingInformation getMatchingMapping(MappingInformation mapping, Message<?> message) {
    String destination = getDestination(message);
    if (mapping.getQueueNames().contains(destination)) {
      return mapping;
    }
    try {
      QueueDetail queueDetail = EndpointRegistry.get(destination);
      if (queueDetail.isSystemGenerated()) {
        queueDetail = EndpointRegistry.get(queueDetail.getPriorityGroup());
        if (mapping.getQueueNames().contains(queueDetail.getName())) {
          return mapping;
        }
      }
    } catch (QueueDoesNotExist e) {
      return null;
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

  public MessageConverter getMessageConverter() {
    return this.messageConverter;
  }
}
