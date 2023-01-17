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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.github.sonus21.TestBase;
import com.github.sonus21.rqueue.CoreUnitTest;
import com.github.sonus21.rqueue.annotation.RqueueHandler;
import com.github.sonus21.rqueue.annotation.RqueueListener;
import com.github.sonus21.rqueue.core.DefaultRqueueMessageConverter;
import com.github.sonus21.rqueue.exception.TimedOutException;
import com.github.sonus21.rqueue.models.Concurrency;
import com.github.sonus21.rqueue.utils.Constants;
import com.github.sonus21.rqueue.utils.TimeoutUtils;
import java.lang.reflect.Method;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.springframework.beans.factory.BeanCreationException;
import org.springframework.context.support.PropertySourcesPlaceholderConfigurer;
import org.springframework.context.support.StaticApplicationContext;
import org.springframework.core.env.MapPropertySource;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageHandler;
import org.springframework.messaging.MessageHeaders;
import org.springframework.messaging.converter.MessageConverter;
import org.springframework.messaging.handler.annotation.MessageExceptionHandler;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.stereotype.Component;

@CoreUnitTest
@SuppressWarnings("unchecked")
class RqueueMessageHandlerTest extends TestBase {

  private static final String testQueue = "test-queue";
  private static final String messagePayloadQueue = "message-queue";
  private static final String smartQueue = "smart-queue";
  private static final String slowQueue = "slow-queue";
  private static final String exceptionQueue = "exception-queue";
  private final String message = "This is a test message.";
  private final MessageConverter messageConverter = new DefaultRqueueMessageConverter();
  private final MessagePayload messagePayload = new MessagePayload(message, message);
  private final String payloadConvertedMessage =
      ((Message<String>) messageConverter.toMessage(messagePayload, null)).getPayload();

  private Message<String> buildMessage(String queueName, String message) {
    return MessageBuilder.createMessage(
        message,
        new MessageHeaders(Collections.singletonMap(RqueueMessageHeaders.DESTINATION, queueName)));
  }

  @Test
  void methodWithStringParameterIsInvoked() {
    StaticApplicationContext applicationContext = new StaticApplicationContext();
    applicationContext.registerSingleton("incomingMessageHandler", IncomingMessageHandler.class);
    applicationContext.registerSingleton("rqueueMessageHandler", RqueueMessageHandler.class);
    applicationContext.refresh();
    MessageHandler messageHandler = applicationContext.getBean(MessageHandler.class);
    messageHandler.handleMessage(buildMessage(testQueue, message));
    IncomingMessageHandler messageListener =
        applicationContext.getBean(IncomingMessageHandler.class);
    assertEquals(message, messageListener.getLastReceivedMessage());
  }

  @Test
  void methodWithMessagePayloadParameterIsInvoked() {
    StaticApplicationContext applicationContext = new StaticApplicationContext();
    applicationContext.registerSingleton("incomingMessageHandler", IncomingMessageHandler.class);
    applicationContext.registerSingleton("rqueueMessageHandler", RqueueMessageHandler.class);
    applicationContext.refresh();
    MessageHandler messageHandler = applicationContext.getBean(MessageHandler.class);
    messageHandler.handleMessage(buildMessage(messagePayloadQueue, payloadConvertedMessage));
    IncomingMessageHandler messageListener =
        applicationContext.getBean(IncomingMessageHandler.class);
    assertEquals(messagePayload, messageListener.getLastReceivedMessage());
  }

  @Test
  void methodWithStringParameterCallExceptionHandler() {
    StaticApplicationContext applicationContext = new StaticApplicationContext();
    applicationContext.registerSingleton("incomingMessageHandler", IncomingMessageHandler.class);
    applicationContext.registerSingleton("rqueueMessageHandler", RqueueMessageHandler.class);
    applicationContext.refresh();

    MessageHandler messageHandler = applicationContext.getBean(MessageHandler.class);

    IncomingMessageHandler messageListener =
        applicationContext.getBean(IncomingMessageHandler.class);
    messageListener.setExceptionHandlerCalled(false);
    try {
      messageHandler.handleMessage(buildMessage(exceptionQueue, message));
    } catch (Exception e) {
      // ignore
    }
    assertTrue(messageListener.isExceptionHandlerCalled());
    assertEquals(message, messageListener.getLastReceivedMessage());
  }

  @Test
  void methodHavingMultipleQueueNames() {
    StaticApplicationContext applicationContext = new StaticApplicationContext();
    applicationContext.registerSingleton("incomingMessageHandler", IncomingMessageHandler.class);
    applicationContext.registerSingleton("rqueueMessageHandler", RqueueMessageHandler.class);
    applicationContext.refresh();

    MessageHandler messageHandler = applicationContext.getBean(MessageHandler.class);

    IncomingMessageHandler messageListener =
        applicationContext.getBean(IncomingMessageHandler.class);
    messageListener.setExceptionHandlerCalled(false);
    messageHandler.handleMessage(buildMessage(slowQueue, message));
    assertEquals(message, messageListener.getLastReceivedMessage());
    messageListener.setLastReceivedMessage(null);
    messageHandler.handleMessage(buildMessage(smartQueue, message + message));
    assertEquals(message + message, messageListener.getLastReceivedMessage());
  }

  @Test
  void methodHavingSpelGettingEvaluated() {
    StaticApplicationContext applicationContext = new StaticApplicationContext();
    applicationContext.registerSingleton("spelMessageHandler", SpelMessageHandler.class);
    applicationContext.registerSingleton("rqueueMessageHandler", RqueueMessageHandler.class);
    applicationContext.refresh();
    MessageHandler messageHandler = applicationContext.getBean(MessageHandler.class);
    SpelMessageHandler messageListener = applicationContext.getBean(SpelMessageHandler.class);
    messageHandler.handleMessage(buildMessage(slowQueue, message));
    assertEquals(message, messageListener.getLastReceivedMessage());
    messageListener.setLastReceivedMessage(null);
    messageHandler.handleMessage(buildMessage(smartQueue, message + message));
    assertEquals(message + message, messageListener.getLastReceivedMessage());
  }

  @Test
  void methodHavingNameFromPropertyFile() {
    StaticApplicationContext applicationContext = new StaticApplicationContext();
    applicationContext.registerSingleton("messageHandler", MessageHandlersWithProperty.class);
    applicationContext.registerSingleton("rqueueMessageHandler", RqueueMessageHandler.class);
    Map<String, Object> map = new HashMap<>();
    map.put("slow.queue.name", slowQueue);
    map.put("smart.queue.name", smartQueue);
    applicationContext
        .getEnvironment()
        .getPropertySources()
        .addLast(new MapPropertySource("test", map));

    applicationContext.registerSingleton("ppc", PropertySourcesPlaceholderConfigurer.class);
    applicationContext.refresh();
    MessageHandler messageHandler = applicationContext.getBean(MessageHandler.class);
    MessageHandlersWithProperty messageListener =
        applicationContext.getBean(MessageHandlersWithProperty.class);
    messageHandler.handleMessage(buildMessage(slowQueue, message));
    assertEquals(message, messageListener.getLastReceivedMessage());
    messageListener.setLastReceivedMessage(null);
    messageHandler.handleMessage(buildMessage(smartQueue, message + message));
    assertEquals(message + message, messageListener.getLastReceivedMessage());
  }

  @Test
  void methodHavingNameFromPropertyFileWithExpression() {
    StaticApplicationContext applicationContext = new StaticApplicationContext();
    applicationContext.registerSingleton(
        "messageHandler", MessageHandlerWithExpressionProperty.class);
    applicationContext.registerSingleton("rqueueMessageHandler", RqueueMessageHandler.class);

    applicationContext
        .getEnvironment()
        .getPropertySources()
        .addLast(new MapPropertySource("test", Collections.singletonMap("queueName", slowQueue)));

    applicationContext.registerSingleton("ppc", PropertySourcesPlaceholderConfigurer.class);
    applicationContext.refresh();
    MessageHandler messageHandler = applicationContext.getBean(MessageHandler.class);
    MessageHandlerWithExpressionProperty messageListener =
        applicationContext.getBean(MessageHandlerWithExpressionProperty.class);
    messageHandler.handleMessage(buildMessage(slowQueue, message));
    assertEquals(message, messageListener.getLastReceivedMessage());
  }

  @Test
  void methodHavingAllPropertiesSet() {
    StaticApplicationContext applicationContext = new StaticApplicationContext();
    applicationContext.registerSingleton("messageHandler", MessageHandlerWithPlaceHolders.class);
    applicationContext.registerSingleton("rqueueMessageHandler", DummyMessageHandler.class);
    Map<String, Object> map = new HashMap<>();
    map.put("queue.name", slowQueue + "," + smartQueue);
    map.put("queue.num.retries", 3);
    map.put("dead.letter.queue.name", slowQueue + "-dlq");
    map.put("queue.dlq.enabled", true);
    map.put("queue.visibility.timeout", "30*30*60");
    map.put("queue.active", "0");
    map.put("queue.concurrency", "10-30");
    map.put("queue.priority", "35");
    map.put("queue.priority.group", "pg");
    map.put("queue.batch.size", "5");
    applicationContext
        .getEnvironment()
        .getPropertySources()
        .addLast(new MapPropertySource("test", map));
    applicationContext.refresh();

    DummyMessageHandler messageHandler = applicationContext.getBean(DummyMessageHandler.class);
    assertEquals(3, messageHandler.mappingInformation.getNumRetry());
    Set<String> queueNames = new HashSet<>();
    queueNames.add(slowQueue);
    queueNames.add(smartQueue);
    assertEquals(queueNames, messageHandler.mappingInformation.getQueueNames());
    assertEquals(30 * 30 * 60L, messageHandler.mappingInformation.getVisibilityTimeout());
    assertEquals(slowQueue + "-dlq", messageHandler.mappingInformation.getDeadLetterQueueName());
    assertTrue(messageHandler.mappingInformation.isDeadLetterConsumerEnabled());
    assertEquals(54000L, messageHandler.mappingInformation.getVisibilityTimeout());
    assertFalse(messageHandler.mappingInformation.isActive());
    assertEquals(new Concurrency(10, 30), messageHandler.mappingInformation.getConcurrency());
    assertEquals(
        Collections.singletonMap(Constants.DEFAULT_PRIORITY_KEY, 35),
        messageHandler.mappingInformation.getPriority());
    assertEquals("pg", messageHandler.mappingInformation.getPriorityGroup());
    assertEquals(5, messageHandler.mappingInformation.getBatchSize());
  }

  @Test
  void concurrencyResolverSingleValue() {
    StaticApplicationContext applicationContext = new StaticApplicationContext();
    applicationContext.registerSingleton("messageHandler", MessageHandlerWithConcurrency.class);
    applicationContext.registerSingleton("rqueueMessageHandler", DummyMessageHandler.class);
    Map<String, Object> map = new HashMap<>();
    map.put("queue.name", slowQueue + "," + smartQueue);
    map.put("queue.concurrency", "5");
    applicationContext
        .getEnvironment()
        .getPropertySources()
        .addLast(new MapPropertySource("test", map));
    applicationContext.refresh();
    DummyMessageHandler messageHandler = applicationContext.getBean(DummyMessageHandler.class);
    assertEquals(new Concurrency(1, 5), messageHandler.mappingInformation.getConcurrency());
  }

  @Test
  void concurrencyResolverMinMax() {
    StaticApplicationContext applicationContext = new StaticApplicationContext();
    applicationContext.registerSingleton("messageHandler", MessageHandlerWithConcurrency.class);
    applicationContext.registerSingleton("rqueueMessageHandler", DummyMessageHandler.class);
    Map<String, Object> map = new HashMap<>();
    map.put("queue.name", slowQueue + "," + smartQueue);
    map.put("queue.concurrency", "5-10");
    applicationContext
        .getEnvironment()
        .getPropertySources()
        .addLast(new MapPropertySource("test", map));
    applicationContext.refresh();
    DummyMessageHandler messageHandler = applicationContext.getBean(DummyMessageHandler.class);
    assertEquals(new Concurrency(5, 10), messageHandler.mappingInformation.getConcurrency());
  }

  @Test
  void multipleMessageHandler() {
    StaticApplicationContext applicationContext = new StaticApplicationContext();
    applicationContext.registerSingleton("messageHandler", MultiMessageHandlerHolders.class);
    applicationContext.registerSingleton("rqueueMessageHandler", RqueueMessageHandler.class);
    Map<String, Object> map = Collections.singletonMap("queue.name", "user-ban");
    applicationContext
        .getEnvironment()
        .getPropertySources()
        .addLast(new MapPropertySource("test", map));
    applicationContext.refresh();
    RqueueMessageHandler rqueueMessageHandler =
        applicationContext.getBean("rqueueMessageHandler", RqueueMessageHandler.class);
    List<RqueueMessageHandler.HandlerMethodWithPrimary> handlerMethodWithPrimaries = null;
    for (Entry<MappingInformation, List<RqueueMessageHandler.HandlerMethodWithPrimary>>
        informationListEntry : rqueueMessageHandler.getHandlerMethodMap().entrySet()) {
      handlerMethodWithPrimaries = informationListEntry.getValue();
    }
    assertNotNull(handlerMethodWithPrimaries);
    assertEquals(4, handlerMethodWithPrimaries.size());
    assertEquals(1, rqueueMessageHandler.getHandlerMethodMap().size());
  }

  @Test
  void multipleMessageHandlerMethodCall() throws TimedOutException {
    StaticApplicationContext applicationContext = new StaticApplicationContext();
    applicationContext.registerSingleton("messageHandler", MultiMessageHandlerHolders.class);
    applicationContext.registerSingleton("rqueueMessageHandler", RqueueMessageHandler.class);
    Map<String, Object> map = Collections.singletonMap("queue.name", "user-ban");
    applicationContext
        .getEnvironment()
        .getPropertySources()
        .addLast(new MapPropertySource("test", map));
    applicationContext.refresh();
    RqueueMessageHandler rqueueMessageHandler =
        applicationContext.getBean("rqueueMessageHandler", RqueueMessageHandler.class);
    MultiMessageHandlerHolders multiMessageHandlerHolders =
        applicationContext.getBean("messageHandler", MultiMessageHandlerHolders.class);

    rqueueMessageHandler.handleMessage(buildMessage("user-ban", "test-data"));
    TimeoutUtils.waitFor(
        () -> 4 == multiMessageHandlerHolders.lastReceivedMessage.size(),
        "all handlers to be invoked");
  }

  @Test
  void priorityResolverMultiLevelQueue() {
    StaticApplicationContext applicationContext = new StaticApplicationContext();
    applicationContext.registerSingleton("messageHandler", MessageHandlerWithPriority.class);
    applicationContext.registerSingleton("rqueueMessageHandler", DummyMessageHandler.class);
    Map<String, Object> map = new HashMap<>();
    map.put("queue.name", slowQueue + "," + smartQueue);
    map.put("queue.priority", "critical=10,high=5,low=2");
    applicationContext
        .getEnvironment()
        .getPropertySources()
        .addLast(new MapPropertySource("test", map));
    applicationContext.refresh();
    DummyMessageHandler messageHandler = applicationContext.getBean(DummyMessageHandler.class);
    Map<String, Integer> priority = new HashMap<>();
    priority.put("critical", 10);
    priority.put("high", 5);
    priority.put("low", 2);
    assertEquals(priority, messageHandler.mappingInformation.getPriority());
  }

  @Test
  void priorityResolverSingleValue() {
    StaticApplicationContext applicationContext = new StaticApplicationContext();
    applicationContext.registerSingleton("messageHandler", MessageHandlerWithPriority.class);
    applicationContext.registerSingleton("rqueueMessageHandler", DummyMessageHandler.class);
    Map<String, Object> map = new HashMap<>();
    map.put("queue.name", slowQueue + "," + smartQueue);
    map.put("queue.priority", "100");
    map.put("queue.priority.group", "pg");
    applicationContext
        .getEnvironment()
        .getPropertySources()
        .addLast(new MapPropertySource("test", map));
    applicationContext.refresh();
    DummyMessageHandler messageHandler = applicationContext.getBean(DummyMessageHandler.class);
    MappingInformation mappingInformation = messageHandler.mappingInformation;
    assertEquals("pg", mappingInformation.getPriorityGroup());
    assertEquals(
        Collections.singletonMap(Constants.DEFAULT_PRIORITY_KEY, 100),
        mappingInformation.getPriority());
  }

  @ParameterizedTest
  @CsvSource({
      "slowQueue1,-1,10-20,10", // default batch size 10
      "slowQueue2,-1,-1,1", // default batch size 1 no concurrency
      "slowQueue3,20,10-20,20", // 20 with concurrency
  })
  void batchSizeResolver(
      String queue, String batchSize, String concurrency, Integer expectedValue) {
    StaticApplicationContext applicationContext = new StaticApplicationContext();
    applicationContext.registerSingleton("messageHandler", MessageHandlerWithBatchSize.class);
    applicationContext.registerSingleton("rqueueMessageHandler", DummyMessageHandler.class);
    Map<String, Object> map = new HashMap<>();
    map.put("queue.name", queue);
    map.put("queue.concurrency", concurrency);
    map.put("queue.batch.size", batchSize);
    applicationContext
        .getEnvironment()
        .getPropertySources()
        .addLast(new MapPropertySource("test", map));
    applicationContext.refresh();
    DummyMessageHandler messageHandler = applicationContext.getBean(DummyMessageHandler.class);
    MappingInformation mappingInformation = messageHandler.mappingInformation;
    assertEquals(expectedValue, mappingInformation.getBatchSize());
  }

  // ////////////////////////////////////////////////////////////////////
  // Invalid config tests
  ///////////////////////////////////////////////////////////////////////
  @Test
  void multipleMessageHandlerWithDuplicateMapping() throws TimedOutException {
    StaticApplicationContext applicationContext = new StaticApplicationContext();
    applicationContext.registerSingleton("messageHandler", MultiMessageHandlerHolders.class);
    applicationContext.registerSingleton("messageHandler2", MessageHandlersWithProperty.class);
    applicationContext.registerSingleton("rqueueMessageHandler", RqueueMessageHandler.class);
    Map<String, Object> map = new HashMap<>();
    map.put("queue.name", slowQueue);
    map.put("slow.queue.name", slowQueue);
    map.put("fast.queue.name", smartQueue);
    applicationContext
        .getEnvironment()
        .getPropertySources()
        .addLast(new MapPropertySource("test", map));
    assertThrows(BeanCreationException.class, applicationContext::refresh);
  }

  @ParameterizedTest
  @CsvSource({
      "\"slowQueue,smartQueue1\",-3", // < 0
      "\"slowQueue,smartQueue2\",high", // number format error
      "\"slowQueue,smartQueue3\",1,1,1", // multiple priority is used
      "\"slowQueue,smartQueue4\",critical=10,high=high", // number format error
  })
  void priorityResolverInvalidValue(String queue, String priority) {
    StaticApplicationContext applicationContext = new StaticApplicationContext();
    applicationContext.registerSingleton("messageHandler", MessageHandlerWithPriority.class);
    applicationContext.registerSingleton("rqueueMessageHandler", DummyMessageHandler.class);
    Map<String, Object> map = new HashMap<>();
    map.put("queue.name", queue);
    map.put("queue.priority", priority);
    applicationContext
        .getEnvironment()
        .getPropertySources()
        .addLast(new MapPropertySource("test", map));
    assertThrows(BeanCreationException.class, applicationContext::refresh);
  }

  @ParameterizedTest
  @CsvSource({
      "smartQueue3, \"\"", // empty value
      "smartQueue4, xyz", // invalid value
      "smartQueue4, 1345", // invalid value
  })
  void activeInvalidValue(String queue, String active) {
    StaticApplicationContext applicationContext = new StaticApplicationContext();
    applicationContext.registerSingleton("messageHandler", MessageHandlerWithConcurrency.class);
    applicationContext.registerSingleton("rqueueMessageHandler", DummyMessageHandler.class);
    Map<String, Object> map = new HashMap<>();
    map.put("queue.name", queue);
    map.put("queue.active", active);
    map.put("queue.concurrency", "10");
    applicationContext
        .getEnvironment()
        .getPropertySources()
        .addLast(new MapPropertySource("test", map));
    assertThrows(BeanCreationException.class, applicationContext::refresh);
  }

  @ParameterizedTest
  @CsvSource({
      "smartQueue1, -1", // < 9
      "smartQueue2, 9", // < 10
      "smartQueue3, high", // number format error
      "smartQueue3, \"\"", // empty value
  })
  void visibilityInvalidValue(String queue, String visibilityTimeout) {
    StaticApplicationContext applicationContext = new StaticApplicationContext();
    applicationContext.registerSingleton("messageHandler", MessageHandlerWithPlaceHolders.class);
    applicationContext.registerSingleton("rqueueMessageHandler", DummyMessageHandler.class);
    Map<String, Object> map = new HashMap<>();
    map.put("queue.name", queue);
    map.put("queue.visibility.timeout", visibilityTimeout);
    map.put("queue.num.retries", "3");
    applicationContext
        .getEnvironment()
        .getPropertySources()
        .addLast(new MapPropertySource("test", map));
    assertThrows(BeanCreationException.class, applicationContext::refresh);
  }

  @ParameterizedTest
  @CsvSource({
      "smartQueue3, high", // number format error
      "smartQueue3, \"\"", // empty value
  })
  void numRetriesInvalidValue(String queue, String numRetries) {
    StaticApplicationContext applicationContext = new StaticApplicationContext();
    applicationContext.registerSingleton("messageHandler", MessageHandlerWithPlaceHolders.class);
    applicationContext.registerSingleton("rqueueMessageHandler", DummyMessageHandler.class);
    Map<String, Object> map = new HashMap<>();
    map.put("queue.name", queue);
    map.put("queue.num.retries", numRetries);
    applicationContext
        .getEnvironment()
        .getPropertySources()
        .addLast(new MapPropertySource("test", map));
    assertThrows(BeanCreationException.class, applicationContext::refresh);
  }

  @ParameterizedTest
  @CsvSource({
      "\"slowQueue{smartQueue1\",-3",
      "\"slowQueue{smartQueue2\",100",
  })
  void queueInvalidValue(String queue, String priority) {
    StaticApplicationContext applicationContext = new StaticApplicationContext();
    applicationContext.registerSingleton("messageHandler", MessageHandlerWithPriority.class);
    applicationContext.registerSingleton("rqueueMessageHandler", DummyMessageHandler.class);
    Map<String, Object> map = new HashMap<>();
    map.put("queue.name", queue);
    map.put("queue.priority", priority);
    applicationContext
        .getEnvironment()
        .getPropertySources()
        .addLast(new MapPropertySource("test", map));
    assertThrows(BeanCreationException.class, applicationContext::refresh);
  }

  @Test
  void duplicatePrimaryHandler() {
    StaticApplicationContext applicationContext = new StaticApplicationContext();
    applicationContext.registerSingleton(
        "messageHandler", MultiMessageWithDuplicatePrimaryHandlerHolders.class);
    applicationContext.registerSingleton("rqueueMessageHandler", RqueueMessageHandler.class);

    Map<String, Object> map = Collections.singletonMap("queue.name", "user-ban");
    applicationContext
        .getEnvironment()
        .getPropertySources()
        .addLast(new MapPropertySource("test", map));
    assertThrows(BeanCreationException.class, applicationContext::refresh);
  }

  @ParameterizedTest
  @CsvSource({
      "\"slowQueue,smartQueue1\",-3", // less than 1 is not allowed
      "\"slowQueue,smartQueue2\",0", // less than 1 is not allowed
      "\"slowQueue,smartQueue3\",foo", // number format error
      "\"slowQueue,smartQueue4\", foo-bar", // number format error after split
      "\"slowQueue,smartQueue5\", 10-5", // min > max
      "\"slowQueue,smartQueue5\", \"\"", // empty concurrency
  })
  void concurrencyInvalidValue(String queue, String concurrency) {
    StaticApplicationContext applicationContext = new StaticApplicationContext();
    applicationContext.registerSingleton("messageHandler", MessageHandlerWithConcurrency.class);
    applicationContext.registerSingleton("rqueueMessageHandler", DummyMessageHandler.class);
    Map<String, Object> map = new HashMap<>();
    map.put("queue.name", queue);
    map.put("queue.concurrency", concurrency);
    applicationContext
        .getEnvironment()
        .getPropertySources()
        .addLast(new MapPropertySource("test", map));
    assertThrows(BeanCreationException.class, applicationContext::refresh);
  }

  @ParameterizedTest
  @CsvSource({
      "\"slowQueue,smartQueue1\",foo, 10-20", // number format error
      "\"slowQueue,smartQueue1\",foo, \"\"", // empty value error
  })
  void batchSizeInvalidValue(String queue, String batchSize, String concurrency) {
    StaticApplicationContext applicationContext = new StaticApplicationContext();
    applicationContext.registerSingleton("messageHandler", MessageHandlerWithBatchSize.class);
    applicationContext.registerSingleton("rqueueMessageHandler", DummyMessageHandler.class);
    Map<String, Object> map = new HashMap<>();
    map.put("queue.name", queue);
    map.put("queue.concurrency", concurrency);
    map.put("queue.batch.size", batchSize);
    applicationContext
        .getEnvironment()
        .getPropertySources()
        .addLast(new MapPropertySource("test", map));
    assertThrows(BeanCreationException.class, applicationContext::refresh);
  }

  /////////////////////////////////////////////////////////////////////////////

  @AllArgsConstructor
  @NoArgsConstructor
  @Data
  public static class MessagePayload {

    private String key;
    private String value;
  }

  @Getter
  @Setter
  private static class IncomingMessageHandler {

    private Object lastReceivedMessage;
    private boolean exceptionHandlerCalled;

    @RqueueListener(value = testQueue)
    public void receive(String value) {
      lastReceivedMessage = value;
    }

    @RqueueListener(value = messagePayloadQueue)
    public void receive(MessagePayload value) {
      lastReceivedMessage = value;
    }

    @RqueueListener(value = exceptionQueue)
    public void exceptionQueue(String message) {
      lastReceivedMessage = message;
      throw new NullPointerException();
    }

    @RqueueListener({slowQueue, smartQueue})
    public void receiveMultiQueue(String value) {
      lastReceivedMessage = value;
    }

    @MessageExceptionHandler(RuntimeException.class)
    public void handleException() {
      exceptionHandlerCalled = true;
    }
  }

  @Getter
  @Setter
  private static class SpelMessageHandler {

    private String lastReceivedMessage;

    @RqueueListener("#{'slow-queue,smart-queue'.split(',')}")
    public void receiveMultiQueue(String value) {
      lastReceivedMessage = value;
    }
  }

  @Getter
  @Setter
  private static class MessageHandlersWithProperty {

    private String lastReceivedMessage;

    @RqueueListener({"${slow.queue.name}", "${smart.queue.name}"})
    public void receiveMultiQueue(String value) {
      lastReceivedMessage = value;
    }
  }

  @Getter
  @Setter
  private static class MessageHandlerWithExpressionProperty {

    private String lastReceivedMessage;

    @RqueueListener("#{environment.queueName}")
    public void receiveMultiQueue(String value) {
      lastReceivedMessage = value;
    }
  }

  @Getter
  @Setter
  private static class MessageHandlerWithPlaceHolders {

    private String lastReceivedMessage;

    @RqueueListener(
        value = "${queue.name}",
        numRetries = "${queue.num.retries}",
        deadLetterQueue = "${dead.letter.queue.name}",
        deadLetterQueueListenerEnabled = "${queue.dlq.enabled:false}",
        visibilityTimeout = "${queue.visibility.timeout}",
        active = "${queue.active:true}",
        concurrency = "${queue.concurrency:-1}",
        priority = "${queue.priority:}",
        priorityGroup = "${queue.priority.group:}",
        batchSize = "${queue.batch.size:-1}")
    public void onMessage(String value) {
      lastReceivedMessage = value;
    }
  }

  @Getter
  @Setter
  @RqueueListener(value = "${queue.name}")
  @Component
  private static class MultiMessageHandlerHolders {

    private Map<String, String> lastReceivedMessage = new ConcurrentHashMap<>();

    @RqueueHandler
    public void onMessage(String value) {
      lastReceivedMessage.put("onMessage", value);
    }

    @RqueueHandler(primary = true)
    public void onMessage2(String value) {
      lastReceivedMessage.put("onMessage2", value);
    }

    @RqueueHandler
    public void onMessage3(String value) {
      lastReceivedMessage.put("onMessage3", value);
    }

    @RqueueHandler
    public void onMessage4(String value) {
      lastReceivedMessage.put("onMessage4", value);
    }
  }

  @Getter
  @Setter
  @RqueueListener(value = "${queue.name}")
  private static class MultiMessageWithDuplicatePrimaryHandlerHolders {

    private String lastReceivedMessage;

    @RqueueHandler(primary = true)
    public void onMessage(String value) {
      lastReceivedMessage = value;
    }

    @RqueueHandler(primary = true)
    public void onMessage2(String value) {
      lastReceivedMessage = value;
    }

    public void onMessage3(String value) {
      lastReceivedMessage = value;
    }

    @RqueueHandler
    public void onMessage4(String value) {
      lastReceivedMessage = value;
    }
  }

  @Getter
  @Setter
  private static class MessageHandlerWithConcurrency {

    private String lastReceivedMessage;

    @RqueueListener(
        value = "${queue.name}",
        concurrency = "${queue.concurrency}",
        active = "${queue.active:true}")
    public void onMessage(String value) {
      lastReceivedMessage = value;
    }
  }

  @Getter
  @Setter
  private static class MessageHandlerWithPriority {

    private String lastReceivedMessage;

    @RqueueListener(
        value = "${queue.name}",
        priority = "${queue.priority}",
        priorityGroup = "${queue.priority.group}")
    public void onMessage(String value) {
      lastReceivedMessage = value;
    }
  }

  @Getter
  @Setter
  private static class MessageHandlerWithBatchSize {

    private String lastReceivedMessage;

    @RqueueListener(
        value = "${queue.name}",
        batchSize = "${queue.batch.size}",
        concurrency = "${queue.concurrency}")
    public void onMessage(String value) {
      lastReceivedMessage = value;
    }
  }

  private static class DummyMessageHandler extends RqueueMessageHandler {

    private MappingInformation mappingInformation;

    public DummyMessageHandler() {
      super(new DefaultRqueueMessageConverter());
    }

    @Override
    protected MappingInformation getMappingForMethod(Method method, Class<?> handlerType) {
      MappingInformation mappingInformation = super.getMappingForMethod(method, handlerType);
      if (method.getName().equals("onMessage")) {
        this.mappingInformation = mappingInformation;
      }
      return mappingInformation;
    }
  }
}
