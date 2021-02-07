/*
 *  Copyright 2021 Sonu Kumar
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *         https://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and limitations under the License.
 *
 */

package com.github.sonus21.rqueue.core;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.github.sonus21.TestBase;
import com.github.sonus21.junit.BootstrapRedis;
import com.github.sonus21.junit.TestQueue;
import com.github.sonus21.rqueue.CoreUnitTest;
import com.github.sonus21.rqueue.common.RqueueRedisTemplate;
import com.github.sonus21.rqueue.core.RedisScriptFactory.ScriptType;
import com.github.sonus21.rqueue.core.impl.RqueueMessageTemplateImpl;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.core.script.DefaultScriptExecutor;
import org.springframework.data.redis.core.script.RedisScript;
import org.springframework.data.redis.core.script.ScriptExecutor;

@CoreUnitTest
@BootstrapRedis(systemRedis = false, port = 6301)
class RedisScriptFactoryTest extends TestBase {
  private final RedisConnectionFactory redisConnectionFactory;
  private final RqueueMessageTemplate rqueueMessageTemplate;

  RedisScriptFactoryTest(RedisConnectionFactory redisConnectionFactory) {
    this.redisConnectionFactory = redisConnectionFactory;
    this.rqueueMessageTemplate = new RqueueMessageTemplateImpl(redisConnectionFactory);
  }

  @Test
  @TestQueue(
      value = {
        "testExpiredMessageMoverWithFailureQueue",
        "__rq::p-queue::testExpiredMessageMoverWithFailureQueue"
      })
  void expiredMessageMoverWithFailureQueue() {
    String queueName = "testExpiredMessageMoverWithFailureQueue";
    String zsetName = "__rq::p-queue::testExpiredMessageMoverWithFailureQueue";
    RqueueMessage rqueueMessage1 = RqueueMessage.builder().message("Test message 1").build();
    RqueueMessage rqueueMessage2 =
        RqueueMessage.builder().message("Test message 2").failureCount(1).build();
    RqueueMessage rqueueMessage3 =
        RqueueMessage.builder().message("Test message 3").failureCount(110).build();
    RqueueMessage rqueueMessage4 = RqueueMessage.builder().message("Test message 4").build();
    rqueueMessageTemplate.addToZset(zsetName, rqueueMessage1, 1000);
    rqueueMessageTemplate.addToZset(zsetName, rqueueMessage2, 1500);
    rqueueMessageTemplate.addToZset(zsetName, rqueueMessage3, 2000);
    rqueueMessageTemplate.addToZset(zsetName, rqueueMessage4, 2500);
    RedisScript<Long> script = RedisScriptFactory.getScript(ScriptType.MOVE_EXPIRED_MESSAGE);
    RqueueRedisTemplate<Long> rqueueRedisTemplate =
        new RqueueRedisTemplate<>(redisConnectionFactory);
    ScriptExecutor<String> scriptExecutor =
        new DefaultScriptExecutor<>(rqueueRedisTemplate.getRedisTemplate());
    scriptExecutor.execute(script, Arrays.asList(queueName, zsetName), 2000, 100, 1);
    List<RqueueMessage> messagesFromList = rqueueMessageTemplate.readFromList(queueName, 0, -1);
    List<RqueueMessage> messagesFromZset = rqueueMessageTemplate.readFromZset(zsetName, 0, -1);
    assertEquals(3, messagesFromList.size());
    assertEquals(1, messagesFromZset.size());
    assertEquals(1, messagesFromList.get(0).getFailureCount());
    assertEquals(2, messagesFromList.get(1).getFailureCount());
    assertEquals(111, messagesFromList.get(2).getFailureCount());
  }

  @Test
  @TestQueue(value = {"testExpiredMessageMover", "__rq::d-queue::testExpiredMessageMover"})
  void expiredMessageMover() {
    String queueName = "testExpiredMessageMover";
    String zsetName = "__rq::d-queue::testExpiredMessageMover";
    RqueueMessage rqueueMessage1 = RqueueMessage.builder().message("Test message 1").build();
    RqueueMessage rqueueMessage2 =
        RqueueMessage.builder().message("Test message 2").failureCount(1).build();
    RqueueMessage rqueueMessage3 =
        RqueueMessage.builder().message("Test message 3").failureCount(110).build();
    RqueueMessage rqueueMessage4 = RqueueMessage.builder().message("Test message 4").build();
    rqueueMessageTemplate.addToZset(zsetName, rqueueMessage1, 1000);
    rqueueMessageTemplate.addToZset(zsetName, rqueueMessage2, 1500);
    rqueueMessageTemplate.addToZset(zsetName, rqueueMessage3, 2000);
    rqueueMessageTemplate.addToZset(zsetName, rqueueMessage4, 2500);
    RedisScript<Long> script = RedisScriptFactory.getScript(ScriptType.MOVE_EXPIRED_MESSAGE);
    RqueueRedisTemplate<Long> rqueueRedisTemplate =
        new RqueueRedisTemplate<>(redisConnectionFactory);
    ScriptExecutor<String> scriptExecutor =
        new DefaultScriptExecutor<>(rqueueRedisTemplate.getRedisTemplate());
    scriptExecutor.execute(script, Arrays.asList(queueName, zsetName), 2000, 100, 0);
    List<RqueueMessage> messagesFromList = rqueueMessageTemplate.readFromList(queueName, 0, -1);
    List<RqueueMessage> messagesFromZset = rqueueMessageTemplate.readFromZset(zsetName, 0, -1);
    assertEquals(3, messagesFromList.size());
    assertEquals(1, messagesFromZset.size());
    assertEquals(0, messagesFromList.get(0).getFailureCount());
    assertEquals(1, messagesFromList.get(1).getFailureCount());
    assertEquals(110, messagesFromList.get(2).getFailureCount());
  }

  @Test
  @TestQueue("testDeleteIfSame")
  void deleteIfSame() {
    String key = "testDeleteIfSame";
    RqueueMessage rqueueMessage = RqueueMessage.builder().message("Test message 1").build();
    RqueueMessage rqueueMessage2 = RqueueMessage.builder().message("Test message 2").build();
    RedisScript<Boolean> script = RedisScriptFactory.getScript(ScriptType.DELETE_IF_SAME);
    RqueueRedisTemplate<RqueueMessage> template =
        new RqueueMessageTemplateImpl(redisConnectionFactory);
    template.set(key, rqueueMessage);
    ScriptExecutor<String> scriptExecutor =
        new DefaultScriptExecutor<>(template.getRedisTemplate());
    assertTrue(template.exist(key));

    // value mismatch
    assertFalse(scriptExecutor.execute(script, Collections.singletonList(key), rqueueMessage2));

    assertTrue(template.exist(key));

    // actual delete
    assertTrue(scriptExecutor.execute(script, Collections.singletonList(key), rqueueMessage));
    assertFalse(template.exist(key));

    // key does not exist test
    assertTrue(scriptExecutor.execute(script, Collections.singletonList(key), rqueueMessage2));

    assertTrue(scriptExecutor.execute(script, Collections.singletonList(key), rqueueMessage));
  }
}
