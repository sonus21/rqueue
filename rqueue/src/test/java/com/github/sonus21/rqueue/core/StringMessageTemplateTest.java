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

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;

import java.util.concurrent.TimeUnit;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.junit.Before;
import org.junit.Test;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.ValueOperations;

public class StringMessageTemplateTest {
  private RedisConnectionFactory redisConnectionFactory = mock(RedisConnectionFactory.class);
  private RedisTemplate<String, String> redisTemplate = mock(RedisTemplate.class);
  private String key = "test-queue";
  private StringMessageTemplate stringMessageTemplate =
      new StringMessageTemplate(redisConnectionFactory);
  private ValueOperations<String, String> valueOperations = mock(ValueOperations.class);

  @Before
  public void setUp() throws Exception {
    FieldUtils.writeField(stringMessageTemplate, "redisTemplate", redisTemplate, true);
  }

  @Test
  public void putIfAbsent() {
    doReturn(valueOperations).when(redisTemplate).opsForValue();
    assertFalse(stringMessageTemplate.putIfAbsent(key, 10L, TimeUnit.SECONDS));

    doReturn(null).when(valueOperations).setIfAbsent(key, key, 11L, TimeUnit.SECONDS);
    assertFalse(stringMessageTemplate.putIfAbsent(key, 11L, TimeUnit.SECONDS));

    doReturn(true).when(valueOperations).setIfAbsent(key, key, 12L, TimeUnit.SECONDS);
    assertTrue(stringMessageTemplate.putIfAbsent(key, 12L, TimeUnit.SECONDS));
  }

  @Test
  public void delete() {
    doReturn(true).when(redisTemplate).delete(key);
    assertTrue(stringMessageTemplate.delete(key));

    doReturn(null).when(redisTemplate).delete(key);
    assertFalse(stringMessageTemplate.delete(key));
  }
}
