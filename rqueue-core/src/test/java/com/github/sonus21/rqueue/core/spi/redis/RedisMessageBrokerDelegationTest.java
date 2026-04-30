/*
 * Copyright (c) 2020-2026 Sonu Kumar
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

package com.github.sonus21.rqueue.core.spi.redis;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import com.github.sonus21.TestBase;
import com.github.sonus21.rqueue.CoreUnitTest;
import com.github.sonus21.rqueue.core.RqueueMessage;
import com.github.sonus21.rqueue.core.RqueueMessageTemplate;
import com.github.sonus21.rqueue.core.spi.Capabilities;
import com.github.sonus21.rqueue.listener.QueueDetail;
import com.github.sonus21.rqueue.models.MessageMoveResult;
import com.github.sonus21.rqueue.utils.TestUtils;
import java.time.Duration;
import java.util.Collections;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.springframework.data.redis.connection.Message;
import org.springframework.data.redis.connection.MessageListener;
import org.springframework.data.redis.core.ListOperations;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.listener.ChannelTopic;
import org.springframework.data.redis.listener.RedisMessageListenerContainer;
import org.springframework.data.redis.listener.Topic;

/**
 * Verifies that {@link RedisMessageBroker} delegates each SPI method to the same {@link
 * RqueueMessageTemplate} call site that the existing public API uses, locking the delegation
 * contract for Phase 2.
 */
@CoreUnitTest
class RedisMessageBrokerDelegationTest extends TestBase {

  private static final QueueDetail QUEUE = TestUtils.createQueueDetail("phase1-broker-test");

  @Mock
  private RqueueMessageTemplate template;

  @Mock
  private RedisTemplate<String, RqueueMessage> redisTemplate;

  @Mock
  private ListOperations<String, RqueueMessage> listOperations;

  @Mock
  private RedisMessageListenerContainer pubSubContainer;

  private RedisMessageBroker broker;

  @BeforeEach
  void setUp() {
    MockitoAnnotations.openMocks(this);
    broker = new RedisMessageBroker(template, pubSubContainer);
  }

  @Test
  void enqueueDelegatesToAddMessage() {
    RqueueMessage m = RqueueMessage.builder().id("a").message("msg").build();
    when(template.addMessage(QUEUE.getQueueName(), m)).thenReturn(1L);

    broker.enqueue(QUEUE, m);

    verify(template).addMessage(QUEUE.getQueueName(), m);
  }

  @Test
  void enqueueWithDelayDelegatesToAddMessageWithDelay() {
    RqueueMessage m = RqueueMessage.builder().id("a").message("msg").build();
    broker.enqueueWithDelay(QUEUE, m, 5000L);

    verify(template)
        .addMessageWithDelay(
            QUEUE.getScheduledQueueName(), QUEUE.getScheduledQueueChannelName(), m);
  }

  @Test
  void popDelegatesToTemplatePop() {
    when(template.pop(any(), any(), any(), anyLong(), anyInt()))
        .thenReturn(Collections.emptyList());

    List<RqueueMessage> out = broker.pop(QUEUE, "consumer", 5, Duration.ofSeconds(1));

    assertNotNull(out);
    verify(template)
        .pop(
            QUEUE.getQueueName(),
            QUEUE.getProcessingQueueName(),
            QUEUE.getProcessingQueueChannelName(),
            QUEUE.getVisibilityTimeout(),
            5);
  }

  @Test
  void ackDelegatesToRemoveElementFromZset() {
    RqueueMessage m = RqueueMessage.builder().id("a").message("msg").build();
    when(template.removeElementFromZset(QUEUE.getProcessingQueueName(), m)).thenReturn(1L);

    assertTrue(broker.ack(QUEUE, m));
    verify(template).removeElementFromZset(QUEUE.getProcessingQueueName(), m);
  }

  @Test
  void ackReturnsFalseWhenNotRemoved() {
    RqueueMessage m = RqueueMessage.builder().id("a").message("msg").build();
    when(template.removeElementFromZset(QUEUE.getProcessingQueueName(), m)).thenReturn(0L);

    assertFalse(broker.ack(QUEUE, m));
  }

  @Test
  void nackWithNoDelayDelegatesToMoveMessage() {
    RqueueMessage m = RqueueMessage.builder().id("a").message("msg").build();
    assertTrue(broker.nack(QUEUE, m, 0L));
    verify(template).moveMessage(QUEUE.getProcessingQueueName(), QUEUE.getQueueName(), m, m);
  }

  @Test
  void nackWithDelayDelegatesToMoveMessageWithDelay() {
    RqueueMessage m = RqueueMessage.builder().id("a").message("msg").build();
    assertTrue(broker.nack(QUEUE, m, 1500L));
    verify(template)
        .moveMessageWithDelay(
            QUEUE.getProcessingQueueName(), QUEUE.getScheduledQueueName(), m, m, 1500L);
  }

  @Test
  void moveExpiredDelegatesToMoveMessageZsetToList() {
    when(template.moveMessageZsetToList(
            eq(QUEUE.getScheduledQueueName()), eq(QUEUE.getQueueName()), eq(10)))
        .thenReturn(new MessageMoveResult(7, true));

    long moved = broker.moveExpired(QUEUE, System.currentTimeMillis(), 10);

    assertEquals(7L, moved);
    verify(template).moveMessageZsetToList(QUEUE.getScheduledQueueName(), QUEUE.getQueueName(), 10);
  }

  @Test
  void peekDelegatesToReadFromList() {
    when(template.readFromList(QUEUE.getQueueName(), 0L, 4L)).thenReturn(Collections.emptyList());

    broker.peek(QUEUE, 0L, 5L);

    verify(template).readFromList(QUEUE.getQueueName(), 0L, 4L);
  }

  @Test
  void sizeUsesUnderlyingListOps() {
    when(template.getTemplate()).thenReturn(redisTemplate);
    when(redisTemplate.opsForList()).thenReturn(listOperations);
    when(listOperations.size(QUEUE.getQueueName())).thenReturn(42L);

    assertEquals(42L, broker.size(QUEUE));
    verify(listOperations).size(QUEUE.getQueueName());
  }

  @Test
  void publishUsesUnderlyingRedisTemplate() {
    when(template.getTemplate()).thenReturn(redisTemplate);

    broker.publish("test-channel", "hello");

    verify(redisTemplate).convertAndSend("test-channel", "hello");
  }

  @Test
  void subscribeRegistersListenerOnContainerAndCloseRemovesIt() throws Exception {
    final String[] received = new String[1];
    AutoCloseable handle = broker.subscribe("ch", s -> received[0] = s);

    // capture listener via verify
    org.mockito.ArgumentCaptor<MessageListener> listenerCaptor =
        org.mockito.ArgumentCaptor.forClass(MessageListener.class);
    org.mockito.ArgumentCaptor<Topic> topicCaptor =
        org.mockito.ArgumentCaptor.forClass(Topic.class);
    verify(pubSubContainer).addMessageListener(listenerCaptor.capture(), topicCaptor.capture());
    assertEquals(
        new ChannelTopic("ch").getTopic(), ((ChannelTopic) topicCaptor.getValue()).getTopic());

    // simulate a delivered message
    Message message = new Message() {
      @Override
      public byte[] getBody() {
        return "payload".getBytes();
      }

      @Override
      public byte[] getChannel() {
        return "ch".getBytes();
      }
    };
    listenerCaptor.getValue().onMessage(message, null);
    assertEquals("payload", received[0]);

    handle.close();
    verify(pubSubContainer, times(1))
        .removeMessageListener(listenerCaptor.getValue(), topicCaptor.getValue());
  }

  @Test
  void capabilitiesAreRedisDefaults() {
    assertEquals(Capabilities.REDIS_DEFAULTS, broker.capabilities());
    assertTrue(broker.capabilities().supportsDelayedEnqueue());
    assertTrue(broker.capabilities().supportsScheduledIntrospection());
    assertTrue(broker.capabilities().supportsCronJobs());
    assertTrue(broker.capabilities().usesPrimaryHandlerDispatch());
  }

  @Test
  void noBrokerStateLeaksToTemplateWhenUnused() {
    // Constructing the broker alone must not call into the template.
    new RedisMessageBroker(template, pubSubContainer);
    verifyNoInteractions(template);
  }
}
