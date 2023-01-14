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

package com.github.sonus21.rqueue.spring.boot.tests.integration;

import com.github.sonus21.junit.RedisAvailable;
import com.github.sonus21.rqueue.exception.TimedOutException;
import com.github.sonus21.rqueue.spring.boot.application.RedisClusterApplication;
import com.github.sonus21.rqueue.spring.boot.tests.SpringBootIntegrationTest;
import com.github.sonus21.rqueue.test.tests.RetryTests;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestPropertySource;

@ContextConfiguration(classes = RedisClusterApplication.class)
@SpringBootTest
@Slf4j
@TestPropertySource(properties = {"rqueue.retry.per.poll=1000", "spring.data.redis.port=8007"})
@RedisAvailable(nodes = {":9000", ":9001", ":9002", ":9003", ":9004", ":9005"})
@SpringBootIntegrationTest
@Tag("redisCluster")
class RedisClusterTest extends RetryTests {

  @Test
  void afterNRetryTaskIsDeletedFromProcessingQueue() throws TimedOutException {
    verifyAfterNRetryTaskIsDeletedFromProcessingQueue();
  }

  @Test
  void messageMovedToDeadLetterQueue() throws TimedOutException {
    verifyMessageMovedToDeadLetterQueue();
  }

  @Test
  void messageIsDiscardedAfterRetries() throws TimedOutException {
    verifyMessageIsDiscardedAfterRetries();
  }
}
