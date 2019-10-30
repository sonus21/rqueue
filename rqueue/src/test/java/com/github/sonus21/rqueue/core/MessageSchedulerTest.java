/*
 * Copyright (c)  2019-2019, Sonu Kumar
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *       https://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */

package com.github.sonus21.rqueue.core;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

import com.github.sonus21.rqueue.utils.QueueInfo;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import lombok.Getter;
import org.junit.Test;
import org.springframework.data.redis.connection.MessageListener;
import org.springframework.data.redis.listener.RedisMessageListenerContainer;
import org.springframework.data.redis.listener.Topic;

public class MessageSchedulerTest {
  private String queueName = "test-queue";
  private MessageQueue messageQueue =
      new MessageQueue(
          QueueInfo.getTimeQueueName("test-queue"),
          "test-queue",
          QueueInfo.getChannelName("test-queue"));
  private MessageQueue messageQueue2 =
      new MessageQueue(
          QueueInfo.getTimeQueueName("test-queue2"),
          "test-queue2",
          QueueInfo.getChannelName("test-queue2"));

  @Test
  public void startWithEmptyList() {
    MessageScheduler messageMoverScheduler =
        new MessageScheduler(
            mock(LongMessageTemplate.class), mock(RedisMessageListenerContainer.class), 1, true);
    messageMoverScheduler.start(Collections.emptyList());
    assertFalse(messageMoverScheduler.isRunning());
  }

  @Test
  public void startWithNonEmptyList() {
    MessageScheduler messageMoverScheduler =
        new MessageScheduler(
            mock(LongMessageTemplate.class), mock(RedisMessageListenerContainer.class), 1, true);
    messageMoverScheduler.start(Collections.singleton(messageQueue));
    assertTrue(messageMoverScheduler.isRunning());
  }

  @Test
  public void startScheduleTaskWithDelay() {
    MessageSchedulerStub messageMoverScheduler = new MessageSchedulerStub();
    messageMoverScheduler.start(Collections.singleton(messageQueue));
    assertEquals(1, messageMoverScheduler.getQueueDetails().size());
    ScheduledTaskDetail scheduledTaskDetail = messageMoverScheduler.getQueueDetails().get(0);
    assertNotNull(scheduledTaskDetail.getFuture());
    assertTrue(scheduledTaskDetail.getStartTime() <= System.currentTimeMillis() + 10);
  }

  @Test
  public void startScheduleMultipleTasksWithDelay() {
    List<MessageQueue> queueNames = new ArrayList<>();
    queueNames.add(messageQueue);
    queueNames.add(messageQueue2);
    MessageSchedulerStub messageMoverScheduler = new MessageSchedulerStub();
    messageMoverScheduler.start(queueNames);
    assertEquals(2, messageMoverScheduler.getQueueDetails().size());
  }

  @Test
  public void startUsesOnlyOneListener() {
    class StubRedisMessageListenerContainer extends RedisMessageListenerContainer {
      private Set<MessageListener> messageListenerSet = new HashSet<>();

      @Override
      public void addMessageListener(MessageListener listener, Topic topic) {
        super.addMessageListener(listener, topic);
        messageListenerSet.add(listener);
      }
    }

    StubRedisMessageListenerContainer redisMessageListenerContainer =
        new StubRedisMessageListenerContainer();
    List<MessageQueue> queueNames = new ArrayList<>();
    queueNames.add(messageQueue2);
    queueNames.add(messageQueue);

    MessageScheduler messageMoverScheduler =
        new MessageScheduler(
            mock(LongMessageTemplate.class), redisMessageListenerContainer, 1, true);
    messageMoverScheduler.start(queueNames);
    assertEquals(1, redisMessageListenerContainer.messageListenerSet.size());
  }

  @Getter
  private static class MessageSchedulerStub extends MessageScheduler {
    private List<ScheduledTaskDetail> queueDetails = new ArrayList<>();

    MessageSchedulerStub() {
      super(mock(LongMessageTemplate.class), mock(RedisMessageListenerContainer.class), 1, true);
    }

    protected ScheduledTaskDetail schedule(
        String queueName, String zsetName, Long startTime, boolean cancelExistingOne) {
      ScheduledTaskDetail scheduledTaskDetail =
          super.schedule(queueName, zsetName, startTime, cancelExistingOne);
      queueDetails.add(scheduledTaskDetail);
      return scheduledTaskDetail;
    }
  }
}
