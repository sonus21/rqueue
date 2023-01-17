/*
 * Copyright (c) 2021-2023 Sonu Kumar
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

package com.github.sonus21.rqueue.core.middleware;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import com.github.sonus21.TestBase;
import com.github.sonus21.rqueue.CoreUnitTest;
import com.github.sonus21.rqueue.common.RqueueRedisTemplate;
import com.github.sonus21.rqueue.core.Job;
import com.github.sonus21.rqueue.listener.QueueDetail;
import com.github.sonus21.rqueue.utils.Constants;
import com.github.sonus21.rqueue.utils.TestUtils;
import com.github.sonus21.rqueue.utils.TimeoutUtils;
import java.time.Duration;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

@CoreUnitTest
class RedisLockMiddlewareTest extends TestBase {

  private final QueueDetail queueDetail = TestUtils.createQueueDetail("test-queue");
  private final String key = "job-xxx";
  private final ExecutorService executor = Executors.newSingleThreadExecutor();
  @Mock
  private Job job;
  @Mock
  private RqueueRedisTemplate<String> redisTemplate;

  @BeforeEach
  public void init() {
    MockitoAnnotations.openMocks(this);
    doReturn(key).when(job).getId();
  }

  @Test
  void handleLockCouldNotAcquireLock() throws Exception {
    doReturn(queueDetail).when(job).getQueueDetail();
    RedisLockMiddleware lockMiddleware =
        new RedisLockMiddleware(redisTemplate) {
          @Override
          protected String getLockIdentifier(Job job) {
            return job.getId();
          }
        };
    doReturn(false).when(redisTemplate).setIfAbsent(key, "1", queueDetail.visibilityDuration());
    lockMiddleware.handle(
        job,
        () -> {
          fail("lock is acquired");
          return null;
        });
    verify(redisTemplate, times(0)).delete(key);
  }

  @Test
  void handleAcquireLock() throws Exception {
    doReturn(queueDetail).when(job).getQueueDetail();
    RedisLockMiddleware lockMiddleware =
        new RedisLockMiddleware(redisTemplate) {
          @Override
          protected String getLockIdentifier(Job job) {
            return job.getId();
          }
        };
    doReturn(true).when(redisTemplate).setIfAbsent(key, "1", queueDetail.visibilityDuration());
    AtomicInteger atomicInteger = new AtomicInteger();
    lockMiddleware.handle(
        job,
        () -> {
          atomicInteger.incrementAndGet();
          return null;
        });
    verify(redisTemplate, times(1)).delete(key);
    assertEquals(1, atomicInteger.get());
  }

  @Test
  void lockCouldNotAcquiredWithDifferentReleaseTime() throws Exception {
    RedisLockMiddleware lockMiddleware =
        new RedisLockMiddleware(redisTemplate) {
          @Override
          protected String getLockIdentifier(Job job) {
            return job.getId();
          }

          @Override
          protected Duration getLockDuration(Job job) {
            return Duration.ofSeconds(5);
          }
        };
    doReturn(true).when(redisTemplate).setIfAbsent(key, "1", Duration.ofSeconds(5));
    AtomicInteger atomicInteger = new AtomicInteger();
    lockMiddleware.handle(
        job,
        () -> {
          atomicInteger.incrementAndGet();
          return null;
        });
    verify(redisTemplate, times(1)).delete(key);
    assertEquals(1, atomicInteger.get());
  }

  @Test
  void handleAcquireLockMultipleJobs() throws Exception {
    AtomicInteger atomicInteger = new AtomicInteger();
    AtomicInteger terminationCounter = new AtomicInteger();
    AtomicInteger lockCounter = new AtomicInteger();
    doReturn(queueDetail).when(job).getQueueDetail();

    RedisLockMiddleware lockMiddleware =
        new RedisLockMiddleware(redisTemplate) {
          @Override
          protected String getLockIdentifier(Job job) {
            return job.getId();
          }
        };
    doAnswer(
        invocation -> {
          int val = lockCounter.incrementAndGet();
          if (val == 1) {
            return Boolean.TRUE;
          }
          return Boolean.FALSE;
        })
        .when(redisTemplate)
        .setIfAbsent(key, "1", queueDetail.visibilityDuration());
    lockMiddleware.handle(
        job,
        () -> {
          executor.submit(
              () -> {
                atomicInteger.incrementAndGet();
                TimeoutUtils.sleep(5 * Constants.ONE_MILLI);
                terminationCounter.incrementAndGet();
              });
          return null;
        });

    lockMiddleware.handle(
        job,
        () -> {
          executor.submit(
              () -> {
                atomicInteger.incrementAndGet();
                TimeoutUtils.sleep(5 * Constants.ONE_MILLI);
                terminationCounter.incrementAndGet();
              });
          return null;
        });
    TimeoutUtils.waitFor(() -> lockCounter.get() == 2, "both lock method to be called");
    TimeoutUtils.waitFor(() -> atomicInteger.get() >= 1, "method to be executed");
    TimeoutUtils.waitFor(() -> terminationCounter.get() >= 1, "handler to terminate");
    verify(redisTemplate, times(1)).delete(key);
  }
}
