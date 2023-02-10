/*
 * Copyright (c) 2019-2023 Sonu Kumar
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

package com.github.sonus21.rqueue.listener;

import static com.github.sonus21.rqueue.core.support.RqueueMessageUtils.cloneMessage;
import static org.springframework.util.Assert.notNull;

import com.github.sonus21.rqueue.annotation.MessageListener;
import com.github.sonus21.rqueue.annotation.RqueueHandler;
import com.github.sonus21.rqueue.annotation.RqueueListener;
import com.github.sonus21.rqueue.core.DefaultRqueueMessageConverter;
import com.github.sonus21.rqueue.core.EndpointRegistry;
import com.github.sonus21.rqueue.exception.QueueDoesNotExist;
import com.github.sonus21.rqueue.models.Concurrency;
import com.github.sonus21.rqueue.utils.Constants;
import com.github.sonus21.rqueue.utils.PriorityUtils;
import com.github.sonus21.rqueue.utils.RetryableRunnable;
import com.github.sonus21.rqueue.utils.ThreadUtils;
import com.github.sonus21.rqueue.utils.ValueResolver;
import com.google.common.annotations.VisibleForTesting;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.core.MethodIntrospector;
import org.springframework.core.annotation.AnnotatedElementUtils;
import org.springframework.core.annotation.AnnotationUtils;
import org.springframework.core.convert.ConversionService;
import org.springframework.core.task.AsyncTaskExecutor;
import org.springframework.format.support.DefaultFormattingConversionService;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessagingException;
import org.springframework.messaging.converter.MessageConverter;
import org.springframework.messaging.handler.HandlerMethod;
import org.springframework.messaging.handler.annotation.support.AnnotationExceptionHandlerMethodResolver;
import org.springframework.messaging.handler.annotation.support.HeaderMethodArgumentResolver;
import org.springframework.messaging.handler.annotation.support.HeadersMethodArgumentResolver;
import org.springframework.messaging.handler.annotation.support.MessageMethodArgumentResolver;
import org.springframework.messaging.handler.annotation.support.PayloadMethodArgumentResolver;
import org.springframework.messaging.handler.invocation.AbstractExceptionHandlerMethodResolver;
import org.springframework.messaging.handler.invocation.AbstractMethodMessageHandler;
import org.springframework.messaging.handler.invocation.HandlerMethodArgumentResolver;
import org.springframework.messaging.handler.invocation.HandlerMethodReturnValueHandler;
import org.springframework.messaging.simp.annotation.support.PrincipalMethodArgumentResolver;
import org.springframework.util.LinkedMultiValueMap;
import org.springframework.util.MultiValueMap;
import org.springframework.util.comparator.ComparableComparator;

@Slf4j
public class RqueueMessageHandler extends AbstractMethodMessageHandler<MappingInformation> {

    private final ConversionService conversionService;
    private final MessageConverter messageConverter;
    private final boolean inspectAllBean;
    private final AsyncTaskExecutor asyncTaskExecutor;
    private final Map<String, MappingInformation> destinationLookup = new HashMap<>(64);
    private final MultiValueMap<MappingInformation, HandlerMethodWithPrimary> handlerMethods =
            new LinkedMultiValueMap<>(64);

    public RqueueMessageHandler(final MessageConverter messageConverter, boolean inspectAllBean) {
        notNull(messageConverter, "messageConverter cannot be null");
        this.messageConverter = messageConverter;
        this.inspectAllBean = inspectAllBean;
        this.conversionService = new DefaultFormattingConversionService();
        this.asyncTaskExecutor =
                ThreadUtils.createTaskExecutor("rqueueMessageExecutor", "multiMessageExecutor-", -1, -1, 0);
    }


    @VisibleForTesting
    public RqueueMessageHandler() {
        this(new DefaultRqueueMessageConverter());
    }

    public RqueueMessageHandler(final MessageConverter messageConverter) {
        this(messageConverter, true);
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
        resolvers.add(new PayloadMethodArgumentResolver(messageConverter));
        return resolvers;
    }


    @Override
    protected List<? extends HandlerMethodReturnValueHandler> initReturnValueHandlers() {
        return new ArrayList<>(getCustomReturnValueHandlers());
    }

    @Override
    public void afterPropertiesSet() {
        super.afterPropertiesSet();
        for (Entry<String, MappingInformation> e : destinationLookup.entrySet()) {
            List<HandlerMethodWithPrimary> handlerMethodWithPrimaries = handlerMethods.get(e.getValue());
            if (handlerMethodWithPrimaries.size() > 1) {
                if (handlerMethodWithPrimaries.stream().filter(m -> m.primary).count() != 1) {
                    logger.error(
                            "There must be exactly one primary listener method, queue: '" + e.getKey() + "'");
                    throw new IllegalStateException("There must be exactly one primary listener method");
                }
            }
        }
    }

    private boolean isMessageHandler(Class<?> beanType) {
        RqueueListener rqueueListener = AnnotationUtils.findAnnotation(beanType, RqueueListener.class);
        if (rqueueListener != null) {
            MappingInformation information = getMappingInformation(rqueueListener);
            Map<Method, MappingInformation> methods =
                    MethodIntrospector.selectMethods(
                            beanType,
                            (MethodIntrospector.MetadataLookup<MappingInformation>)
                                    method -> getMappingInformation(method, information));
            Object handler = Objects.requireNonNull(getBeanFactory()).getBean(beanType);
            methods.forEach((key, value) -> registerHandlerMethod(handler, key, value));
        }
        return rqueueListener != null;
    }

    @Override
    protected boolean isHandler(Class<?> beanType) {
        if (isMessageHandler(beanType)) {
            return false;
        }
        if (inspectAllBean) {
            return true;
        }
        return AnnotatedElementUtils.hasAnnotation(beanType, MessageListener.class);
    }

    public MultiValueMap<MappingInformation, HandlerMethodWithPrimary> getHandlerMethodMap() {
        return handlerMethods;
    }

    private void addMatchesToCollection(
            MappingInformation mapping, Message<?> message, Set<Match> matches) {
        MappingInformation match = getMatchingMapping(mapping, message);
        if (match != null) {
            for (HandlerMethodWithPrimary method : getHandlerMethodMap().get(mapping)) {
                matches.add(new Match(match, method));
            }
        }
    }

    @Override
    protected void registerHandlerMethod(Object handler, Method method, MappingInformation mapping) {
        for (String pattern : getDirectLookupDestinations(mapping)) {
            MappingInformation oldMapping = destinationLookup.get(pattern);
            if (oldMapping != null && !oldMapping.equals(mapping)) {
                List<HandlerMethodWithPrimary> methods = handlerMethods.get(oldMapping);
                throw new IllegalStateException(
                        "More than one listeners are registered to same queue\n"
                                + "Existing Methods "
                                + methods
                                + "\nNew Method: ["
                                + method
                                + "]");
            }
            this.destinationLookup.put(pattern, mapping);
        }
        this.handlerMethods.add(
                mapping,
                new HandlerMethodWithPrimary(createHandlerMethod(handler, method), mapping.isPrimary()));
    }

    @Override
    protected void handleMessageInternal(Message<?> message, String lookupDestination) {
        MappingInformation mapping = this.destinationLookup.get(lookupDestination);
        Set<Match> matches = new HashSet<>();
        addMatchesToCollection(mapping, message, matches);
        if (matches.isEmpty()) {
            handleNoMatch(this.getHandlerMethodMap().keySet(), lookupDestination, message);
            return;
        }
        executeMatches(matches, message, lookupDestination);
    }

    private void executeMultipleMatch(
            List<Match> matches, Message<?> message, String lookupDestination) {
        Match primaryMatch = null;
        for (Match match : matches) {
            if (match.handlerMethod.primary) {
                primaryMatch = match;
            }
        }
        if (primaryMatch == null) {
            throw new IllegalStateException("At least one of them must be primary");
        }
        for (Match match : matches) {
            if (!match.handlerMethod.primary) {
                Message<?> clonedMessage = cloneMessage(message);
                asyncTaskExecutor.execute(new MultiHandler(match, clonedMessage, lookupDestination));
            }
        }
        handleMatch(
                primaryMatch.information, primaryMatch.handlerMethod.method, lookupDestination, message);
    }

    private void executeMatches(Set<Match> matchesIn, Message<?> message, String lookupDestination) {
        List<Match> matches = new ArrayList<>(matchesIn);
        if (matches.size() == 1) {
            Match match = matches.get(0);
            handleMatch(match.information, match.handlerMethod.method, lookupDestination, message);
        } else {
            executeMultipleMatch(matches, message, lookupDestination);
        }
    }

    private MappingInformation getMappingInformation(
            Method method, MappingInformation mappingInformation) {
        RqueueHandler rqueueHandler = AnnotationUtils.findAnnotation(method, RqueueHandler.class);
        if (rqueueHandler == null) {
            return null;
        }
        return mappingInformation.toBuilder().primary(rqueueHandler.primary()).build();
    }

    private MappingInformation getMappingInformation(RqueueListener rqueueListener) {
        Set<String> queueNames = resolveQueueNames(rqueueListener);
        String deadLetterQueueName = resolveDeadLetterQueue(rqueueListener);
        int numRetries = resolveNumRetries(rqueueListener);
        long visibilityTimeout = resolveVisibilityTimeout(rqueueListener);
        boolean active = isActive(rqueueListener);
        boolean consumerEnabled = resolveConsumerEnabled(rqueueListener);
        Concurrency concurrency = resolveConcurrency(rqueueListener);
        Map<String, Integer> priorityMap = resolvePriority(rqueueListener);
        String priorityGroup = resolvePriorityGroup(rqueueListener);
        int batchSize = getBatchSize(rqueueListener, concurrency);
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
                        .batchSize(batchSize)
                        .build();
        if (mappingInformation.isValid()) {
            return mappingInformation;
        }
        logger.warn("Invalid Queue '" + mappingInformation + "' configuration");
        return null;
    }

    private int getBatchSize(RqueueListener rqueueListener, Concurrency concurrency) {
        int val =
                ValueResolver.resolveKeyToInteger(getApplicationContext(), rqueueListener.batchSize());
        // batch size is not set
        if (val < Constants.MIN_BATCH_SIZE) {
            // concurrency is set but batch size is not set, use default batch size
            if (concurrency.isValid()) {
                val = Constants.BATCH_SIZE_FOR_CONCURRENCY_BASED_LISTENER;
            } else {
                val = Constants.MIN_BATCH_SIZE;
            }
        }
        return val;
    }

    @Override
    protected MappingInformation getMappingForMethod(Method method, Class<?> handlerType) {
        RqueueListener rqueueListener = AnnotationUtils.findAnnotation(method, RqueueListener.class);
        if (rqueueListener != null) {
            return getMappingInformation(rqueueListener);
        }
        return null;
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
        if (lowerLimit < Constants.MIN_CONCURRENCY) {
            throw new IllegalStateException(
                    "lower limit of concurrency must be greater than or equal to "
                            + Constants.MIN_CONCURRENCY);
        }
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

    private boolean resolveConsumerEnabled(RqueueListener rqueueListener) {
        return ValueResolver.resolveToBoolean(
                getApplicationContext(), rqueueListener.deadLetterQueueListenerEnabled());
    }

    private long resolveVisibilityTimeout(RqueueListener rqueueListener) {
        long value =
                ValueResolver.resolveKeyToLong(getApplicationContext(), rqueueListener.visibilityTimeout());
        if (value < Constants.MIN_VISIBILITY) {
            throw new IllegalStateException(
                    "Visibility  must be greater than or equal to " + Constants.MIN_VISIBILITY);
        }
        return value;
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

    private void checkInvalidQueueName(Set<String> queueNames) {
        List<String> invalidNames = new LinkedList<>();
        Character[] invalidChars =
                new Character[]{
                        '{', '}', ' ', '<', '>',
                };
        for (String queue : queueNames) {
            for (int i = 0; i < queue.length(); i++) {
                for (char invalidChar : invalidChars) {
                    if (queue.charAt(i) == invalidChar) {
                        invalidNames.add(queue);
                        break;
                    }
                }
            }
        }
        if (!invalidNames.isEmpty()) {
            String invalidCharsStr =
                    Stream.of(invalidChars)
                            .map(e -> String.format("'%c'", e))
                            .collect(Collectors.joining(Constants.Comma));
            String queueNamesStr =
                    invalidNames.stream()
                            .map(e -> String.format("'%s'", e))
                            .collect(Collectors.joining(Constants.Comma));
            String message =
                    String.format(
                            "Queue name contains invalid char%n Not Allowed Chars [%s] %n Queues: [%s]",
                            invalidCharsStr, queueNamesStr);
            throw new IllegalStateException(message);
        }
    }

    private Set<String> resolveQueueNames(RqueueListener rqueueListener) {
        String[] queueNames = rqueueListener.value();
        Set<String> result = new HashSet<>(queueNames.length);
        for (String queueName : queueNames) {
            result.addAll(
                    Arrays.asList(
                            ValueResolver.resolveKeyToArrayOfStrings(getApplicationContext(), queueName)));
        }
        checkInvalidQueueName(result);
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

    @AllArgsConstructor
    static class HandlerMethodWithPrimary {

        HandlerMethod method;
        boolean primary;

        @Override
        public String toString() {
            return method.toString();
        }
    }

    @AllArgsConstructor
    @EqualsAndHashCode
    private static class Match {

        private final MappingInformation information;
        private final HandlerMethodWithPrimary handlerMethod;
    }

    private class MultiHandler extends RetryableRunnable<Object> {

        private final Match match;
        private final Message<?> message;
        private final String lookupDestination;

        protected MultiHandler(Match match, Message<?> message, String lookupDestination) {
            super(log, "");
            this.match = match;
            this.message = message;
            this.lookupDestination = lookupDestination;
        }

        @Override
        public void start() {
            handleMatch(match.information, match.handlerMethod.method, lookupDestination, message);
        }
    }
}
