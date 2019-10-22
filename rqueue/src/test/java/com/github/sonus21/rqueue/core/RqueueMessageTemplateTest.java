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
import static org.junit.Assert.assertNull;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;

import java.util.Collections;
import java.util.concurrent.TimeUnit;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.core.DefaultTypedTuple;
import org.springframework.data.redis.core.ListOperations;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.ZSetOperations;

@RunWith(MockitoJUnitRunner.StrictStubs.class)
public class RqueueMessageTemplateTest {
  private RedisConnectionFactory redisConnectionFactory = mock(RedisConnectionFactory.class);
  private RedisTemplate<String, RqueueMessage> redisTemplate = mock(RedisTemplate.class);
  private RqueueMessageTemplate rqueueMessageTemplate =
      new RqueueMessageTemplate(redisConnectionFactory);
  private ListOperations<String, RqueueMessage> listOperations = mock(ListOperations.class);
  private ZSetOperations<String, RqueueMessage> zSetOperations = mock(ZSetOperations.class);
  private String key = "test-queue";
  private RqueueMessage message = new RqueueMessage(key, "This is a message", null, 100L);
  private DefaultTypedTuple<RqueueMessage> typedTuple =
      new DefaultTypedTuple(message, (double) message.getProcessAt());

  @Before
  public void init() throws Exception {
    FieldUtils.writeField(rqueueMessageTemplate, "redisTemplate", redisTemplate, true);
  }

  @Test
  public void add() {
    doReturn(listOperations).when(redisTemplate).opsForList();
    doReturn(1L).when(listOperations).rightPush(key, message);
    rqueueMessageTemplate.add(key, message);
  }

  @Test
  public void lpop() {
    doReturn(listOperations).when(redisTemplate).opsForList();
    doReturn(message).when(listOperations).leftPop(key, 10, TimeUnit.SECONDS);
    assertEquals(message, rqueueMessageTemplate.lpop(key));
    doReturn(null).when(listOperations).leftPop(key, 10, TimeUnit.SECONDS);
    assertNull(rqueueMessageTemplate.lpop(key));
  }

  @Test
  public void addToZset() {
    doReturn(zSetOperations).when(redisTemplate).opsForZSet();
    doReturn(true).when(zSetOperations).add(key, message, message.getProcessAt());
    rqueueMessageTemplate.addToZset(key, message);
  }

  @Test
  public void removeFromZset() {
    doReturn(zSetOperations).when(redisTemplate).opsForZSet();
    doReturn(1L).when(zSetOperations).remove(key, message);
    rqueueMessageTemplate.removeFromZset(key, message);
  }

  @Test
  public void getFirstFromZset() {
    doReturn(zSetOperations).when(redisTemplate).opsForZSet();
    doReturn(null).when(zSetOperations).rangeWithScores(key, 0, 0);
    assertNull(rqueueMessageTemplate.getFirstFromZset(key));

    // empty collection
    doReturn(Collections.emptySet()).when(zSetOperations).rangeWithScores(key, 0, 0);
    assertNull(rqueueMessageTemplate.getFirstFromZset(key));

    // only one element
    doReturn(Collections.singleton(typedTuple)).when(zSetOperations).rangeWithScores(key, 0, 0);
    assertEquals(message, rqueueMessageTemplate.getFirstFromZset(key));
  }
}
