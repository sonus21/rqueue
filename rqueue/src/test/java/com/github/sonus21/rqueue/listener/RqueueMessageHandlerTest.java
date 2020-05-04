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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import com.github.sonus21.rqueue.annotation.RqueueListener;
import com.github.sonus21.rqueue.converter.GenericMessageConverter;
import com.github.sonus21.rqueue.utils.QueueUtils;
import java.lang.reflect.Method;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;
import org.springframework.context.support.PropertySourcesPlaceholderConfigurer;
import org.springframework.context.support.StaticApplicationContext;
import org.springframework.core.env.MapPropertySource;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageHandler;
import org.springframework.messaging.handler.annotation.MessageExceptionHandler;
import org.springframework.messaging.support.GenericMessage;

@RunWith(MockitoJUnitRunner.StrictStubs.class)
public class RqueueMessageHandlerTest {
  private static final String testQueue = "test-queue";
  private static final String messagePayloadQueue = "message-queue";
  private static final String smartQueue = "smart-queue";
  private static final String slowQueue = "slow-queue";
  private static final String exceptionQueue = "exception-queue";
  private String message = "This is a test message.";
  private GenericMessageConverter messageConverter = new GenericMessageConverter();
  private MessagePayload messagePayload = new MessagePayload(message, message);
  private String payloadConvertedMessage =
      ((Message<String>) messageConverter.toMessage(messagePayload, null)).getPayload();

  private Message<String> buildMessage(String queueName, String message) {
    return new GenericMessage<>(
        message, Collections.singletonMap(QueueUtils.getMessageHeaderKey(), queueName));
  }

  @Test
  public void testMethodWithStringParameterIsInvoked() {
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
  public void testMethodWithMessagePayloadParameterIsInvoked() {
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
  public void testMethodWithStringParameterCallExceptionHandler() {
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
  public void testMethodHavingMultipleQueueNames() {
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
  public void testMethodHavingSpelGettingEvaluated() {
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
  public void testMethodHavingNameFromPropertyFile() {
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
  public void testMethodHavingNameFromPropertyFileWithExpression() {
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
  public void testMethodHavingAllPropertiesSet() {
    StaticApplicationContext applicationContext = new StaticApplicationContext();
    applicationContext.registerSingleton("messageHandler", MessageHandlerWithPlaceHolders.class);
    applicationContext.registerSingleton("rqueueMessageHandler", DummyMessageHandler.class);
    Map<String, Object> map = new HashMap<>();
    map.put("queue.name", slowQueue + "," + smartQueue);
    map.put("queue.dead.letter.queue", true);
    map.put("queue.num.retries", 3);
    map.put("queue.visibility.timeout", "30*30*60");
    map.put("dead.letter.queue.name", slowQueue + "-dlq");
    applicationContext
        .getEnvironment()
        .getPropertySources()
        .addLast(new MapPropertySource("test", map));
    applicationContext.refresh();

    DummyMessageHandler messageHandler = applicationContext.getBean(DummyMessageHandler.class);
    assertTrue(messageHandler.mappingInformation.isDelayedQueue());
    assertEquals(3, messageHandler.mappingInformation.getNumRetries());
    Set<String> queueNames = new HashSet<>();
    queueNames.add(slowQueue);
    queueNames.add(smartQueue);
    assertEquals(queueNames, messageHandler.mappingInformation.getQueueNames());
    assertEquals(30 * 30 * 60L, messageHandler.mappingInformation.getVisibilityTimeout());
    assertEquals(slowQueue + "-dlq", messageHandler.mappingInformation.getDeadLetterQueueName());
  }

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
        delayedQueue = "${queue.dead.letter.queue}",
        numRetries = "${queue.num.retries}",
        deadLetterQueue = "${dead.letter.queue.name}",
        visibilityTimeout = "${queue.visibility.timeout}")
    public void onMessage(String value) {
      lastReceivedMessage = value;
    }
  }

  private static class DummyMessageHandler extends RqueueMessageHandler {
    private MappingInformation mappingInformation;

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
