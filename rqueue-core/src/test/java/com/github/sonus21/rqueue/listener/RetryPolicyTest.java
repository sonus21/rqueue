/*
 * Copyright (c) 2026 Sonu Kumar
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
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.doReturn;

import com.github.sonus21.rqueue.CoreUnitTest;
import com.github.sonus21.rqueue.config.RqueueConfig;
import com.github.sonus21.rqueue.core.RqueueMessage;
import com.github.sonus21.rqueue.utils.TestUtils;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

@CoreUnitTest
class RetryPolicyTest {

  private final QueueDetail queueDetail = TestUtils.createQueueDetail("queue", 2, 900000L, null);

  @Test
  void retryCountForPollUsesRemainingRetryBudget() {
    RqueueConfig rqueueConfig = Mockito.mock(RqueueConfig.class);
    doReturn(100).when(rqueueConfig).getRetryPerPoll();
    RqueueMessage rqueueMessage = new RqueueMessage();

    assertEquals(1, RetryPolicy.retryCountForPoll(rqueueConfig, rqueueMessage, queueDetail, 1));
  }

  @Test
  void retryCountForPollKeepsExplicitMessageRetryCount() {
    RqueueConfig rqueueConfig = Mockito.mock(RqueueConfig.class);
    doReturn(-1).when(rqueueConfig).getRetryPerPoll();
    RqueueMessage rqueueMessage = RqueueMessage.builder().retryCount(1000).build();

    assertEquals(
        999, RetryPolicy.retryCountForPoll(rqueueConfig, rqueueMessage, queueDetail, 1));
  }

  @Test
  void isExhaustedUsesEffectiveRetryCount() {
    RqueueMessage rqueueMessage = new RqueueMessage();

    assertFalse(RetryPolicy.isExhausted(rqueueMessage, queueDetail, 1));
    assertTrue(RetryPolicy.isExhausted(rqueueMessage, queueDetail, 2));
  }

  @Test
  void retryForeverSentinelUsesFiniteLimit() {
    QueueDetail retryForeverQueue =
        TestUtils.createQueueDetail("queue", Integer.MAX_VALUE, 900000L, null);
    RqueueMessage rqueueMessage = new RqueueMessage();

    assertEquals(RetryPolicy.UNLIMITED_RETRY_LIMIT, RetryPolicy.maxRetryCount(
        rqueueMessage, retryForeverQueue));
    assertEquals(
        1,
        RetryPolicy.remainingRetryCount(
            rqueueMessage, retryForeverQueue, RetryPolicy.UNLIMITED_RETRY_LIMIT - 1));
    assertTrue(RetryPolicy.isExhausted(
        rqueueMessage, retryForeverQueue, RetryPolicy.UNLIMITED_RETRY_LIMIT));
  }
}
