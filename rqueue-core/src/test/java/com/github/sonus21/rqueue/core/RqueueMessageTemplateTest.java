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

package com.github.sonus21.rqueue.core;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import com.github.sonus21.TestBase;
import com.github.sonus21.rqueue.CoreUnitTest;
import com.github.sonus21.rqueue.utils.Constants;
import com.github.sonus21.rqueue.utils.TestUtils;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.core.ListOperations;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.script.DefaultScriptExecutor;

@CoreUnitTest
class RqueueMessageTemplateTest extends TestBase {

  private final String queueName = "test-queue";
  private final RqueueMessage message =
      RqueueMessage.builder()
          .queuedTime(System.nanoTime())
          .id(UUID.randomUUID().toString())
          .queueName(queueName)
          .message("This is a test message")
          .processAt(System.currentTimeMillis())
          .build();
  @Mock
  private RedisConnectionFactory redisConnectionFactory;
  @Mock
  private RedisTemplate<String, RqueueMessage> redisTemplate;
  @Mock
  private ListOperations<String, RqueueMessage> listOperations;
  @Mock
  private DefaultScriptExecutor<String> scriptExecutor;
  private RqueueMessageTemplate rqueueMessageTemplate;

  @BeforeEach
  public void init() throws Exception {
    MockitoAnnotations.openMocks(this);
    rqueueMessageTemplate = TestUtils.rqueueMessageTemplate(redisConnectionFactory, redisTemplate,
        scriptExecutor);
  }

  @Test
  void add() {
    doReturn(listOperations).when(redisTemplate).opsForList();
    doReturn(1L).when(listOperations).rightPush(queueName, message);
    rqueueMessageTemplate.addMessage(queueName, message);
  }

  @Test
  void popN() {
    rqueueMessageTemplate.pop(
        queueName, queueName + "rq", queueName + "dq", Constants.DELTA_BETWEEN_RE_ENQUEUE_TIME, 3);
    verify(scriptExecutor, times(1)).execute(any(), any(), any());
  }

  @Test
  void addWithDelay() {
    rqueueMessageTemplate.addMessageWithDelay(queueName, queueName + "rq", message);
    verify(scriptExecutor, times(1)).execute(any(), any(), any());
  }

  @Test
  void moveMessage() {
    List<String> args = new ArrayList<>();
    args.add("dlq" + queueName);
    args.add(queueName);
    doReturn(70L).when(scriptExecutor).execute(any(), eq(args), eq(100L));
    doReturn(20L).when(scriptExecutor).execute(any(), eq(args), eq(50L));
    rqueueMessageTemplate.moveMessageListToList(args.get(0), args.get(1), 150);
    verify(scriptExecutor, times(1)).execute(any(), eq(args), eq(100L));
    verify(scriptExecutor, times(1)).execute(any(), eq(args), eq(50L));
  }

  @Test
  void moveMessage2() {
    List<String> args = new ArrayList<>();
    args.add("dlq" + queueName);
    args.add(queueName);
    doReturn(0L).when(scriptExecutor).execute(any(), eq(args), eq(100L));
    rqueueMessageTemplate.moveMessageListToList(args.get(0), args.get(1), 150);
    verify(scriptExecutor, times(1)).execute(any(), eq(args), eq(100L));
  }

  @Test
  void moveMessageAcrossZset() {
    List<String> args = new ArrayList<>();
    args.add("zset1-" + queueName);
    args.add("zset2-" + queueName);
    doReturn(0L).when(scriptExecutor).execute(any(), eq(args), eq(100L), eq(10L), eq(false));
    rqueueMessageTemplate.moveMessageZsetToZset(args.get(0), args.get(1), 150, 10L, false);
  }

  @Test
  void moveMessageAcrossZset2() {
    List<String> args = new ArrayList<>();
    args.add("zset1-" + queueName);
    args.add("zset2-" + queueName);
    long score = System.currentTimeMillis();
    doReturn(70L).when(scriptExecutor).execute(any(), eq(args), eq(100L), eq(score), eq(true));
    doReturn(20L).when(scriptExecutor).execute(any(), eq(args), eq(50L), eq(score), eq(true));
    rqueueMessageTemplate.moveMessageZsetToZset(args.get(0), args.get(1), 150, score, true);
  }
}
