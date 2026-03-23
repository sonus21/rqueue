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

package com.github.sonus21.rqueue.worker;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import com.github.sonus21.TestBase;
import com.github.sonus21.rqueue.CoreUnitTest;
import com.github.sonus21.rqueue.common.RqueueRedisTemplate;
import com.github.sonus21.rqueue.config.RqueueConfig;
import com.github.sonus21.rqueue.listener.QueueDetail;
import com.github.sonus21.rqueue.models.Concurrency;
import com.github.sonus21.rqueue.models.registry.RqueueWorkerInfo;
import com.github.sonus21.rqueue.models.registry.RqueueWorkerPollerMetadata;
import com.github.sonus21.rqueue.models.registry.RqueueWorkerPollerView;
import com.github.sonus21.rqueue.utils.RedisUtils;
import com.github.sonus21.rqueue.utils.SerializationUtils;
import java.lang.reflect.Field;
import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.springframework.core.task.SimpleAsyncTaskExecutor;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.core.RedisTemplate;
import tools.jackson.databind.ObjectMapper;

@CoreUnitTest
class RqueueWorkerRegistryImplTest extends TestBase {

  private final RedisConnectionFactory redisConnectionFactory = null;
  private TestWorkerTemplate workerTemplate;
  private TestStringTemplate stringTemplate;
  private final RedisTemplate<String, Object> redisTemplate = new RedisTemplate<>();

  private RedisUtils.RedisTemplateProvider originalRedisTemplateProvider;
  private final ObjectMapper objectMapper = SerializationUtils.createObjectMapper();

  @BeforeEach
  void init() {
    MockitoAnnotations.openMocks(this);
    redisTemplate.setConnectionFactory(Mockito.mock(RedisConnectionFactory.class));
    originalRedisTemplateProvider = RedisUtils.redisTemplateProvider;
    RedisUtils.redisTemplateProvider =
        new RedisUtils.RedisTemplateProvider() {
          @Override
          public <V> RedisTemplate<String, V> getRedisTemplate(
              RedisConnectionFactory redisConnectionFactory) {
            return (RedisTemplate<String, V>) redisTemplate;
          }
        };
    workerTemplate = new TestWorkerTemplate();
    stringTemplate = new TestStringTemplate();
  }

  @AfterEach
  void cleanup() {
    RedisUtils.redisTemplateProvider = originalRedisTemplateProvider;
  }

  @Test
  void getQueueWorkersUsesCapacityExhaustedAsActivity() throws Exception {
    RqueueConfig rqueueConfig = new RqueueConfig(redisConnectionFactory, null, false, 2);
    rqueueConfig.setWorkerRegistryEnabled(true);
    rqueueConfig.setWorkerRegistryQueueHeartbeatIntervalInSeconds(15);
    rqueueConfig.setWorkerRegistryQueueTtlInSeconds(3600);
    RqueueWorkerRegistryImpl registry = new RqueueWorkerRegistryImpl(rqueueConfig);
    setField(registry, "workerTemplate", workerTemplate);
    setField(registry, "stringTemplate", stringTemplate);

    long now = System.currentTimeMillis();
    String workerId = "worker-1";
    RqueueWorkerPollerMetadata metadata =
        RqueueWorkerPollerMetadata.builder()
            .workerId(workerId)
            .lastPollAt(now - Duration.ofSeconds(40).toMillis())
            .lastCapacityExhaustedAt(now)
            .capacityExhaustedCount(2L)
            .build();
    stringTemplate.values =
        Collections.singletonMap(workerId, objectMapper.writeValueAsString(metadata));
    workerTemplate.values =
        Collections.singletonList(
            RqueueWorkerInfo.builder()
                .workerId(workerId)
                .host("host-1")
                .pid("123")
                .startedAt(now - Duration.ofMinutes(5).toMillis())
                .lastSeenAt(now)
                .build());

    List<RqueueWorkerPollerView> workers = registry.getQueueWorkers("test");

    assertEquals(1, workers.size());
    assertEquals("ACTIVE", workers.get(0).getStatus());
    assertEquals(2L, workers.get(0).getCapacityExhaustedCount());
    assertEquals("host-1", workers.get(0).getHost());
    assertFalse(workers.get(0).getLastCapacityExhaustedAge().isEmpty());
  }

  @Test
  void recordQueueCapacityExhaustedSaturatesAtLongMaxValue() throws Exception {
    RqueueConfig rqueueConfig = new RqueueConfig(redisConnectionFactory, null, false, 2);
    rqueueConfig.setWorkerRegistryEnabled(true);
    rqueueConfig.setWorkerRegistryQueueHeartbeatIntervalInSeconds(15);
    rqueueConfig.setWorkerRegistryQueueTtlInSeconds(3600);
    RqueueWorkerRegistryImpl registry = new RqueueWorkerRegistryImpl(rqueueConfig);
    setField(registry, "workerTemplate", workerTemplate);
    setField(registry, "stringTemplate", stringTemplate);
    setField(
        registry,
        "capacityExhaustedCountByQueue",
        new java.util.concurrent.ConcurrentHashMap<>(
            Collections.singletonMap("test-queue", Long.MAX_VALUE)));

    QueueDetail queueDetail =
        QueueDetail.builder()
            .name("test-queue")
            .queueName("test-queue")
            .processingQueueName("test-queue-processing")
            .processingQueueChannelName("test-queue-processing-channel")
            .scheduledQueueName("test-queue-scheduled")
            .scheduledQueueChannelName("test-queue-scheduled-channel")
            .completedQueueName("test-queue-completed")
            .active(true)
            .visibilityTimeout(1000L)
            .batchSize(1)
            .numRetry(3)
            .concurrency(new Concurrency(1, 1))
            .priority(Collections.emptyMap())
            .build();

    registry.recordQueueCapacityExhausted(
        queueDetail,
        new com.github.sonus21.rqueue.utils.QueueThreadPool(
            new SimpleAsyncTaskExecutor(), false, 1));

    RqueueWorkerPollerMetadata metadata =
        objectMapper.readValue(stringTemplate.lastHashValue, RqueueWorkerPollerMetadata.class);

    assertEquals(Long.MAX_VALUE, metadata.getCapacityExhaustedCount());
  }

  private static void setField(Object target, String fieldName, Object value) throws Exception {
    Field field = target.getClass().getDeclaredField(fieldName);
    field.setAccessible(true);
    field.set(target, value);
  }

  private static class TestWorkerTemplate extends RqueueRedisTemplate<RqueueWorkerInfo> {
    private List<RqueueWorkerInfo> values = Collections.emptyList();
    private RqueueWorkerInfo lastValue;

    TestWorkerTemplate() {
      super(null);
    }

    @Override
    public List<RqueueWorkerInfo> mget(java.util.Collection<String> keys) {
      return values;
    }

    @Override
    public void set(String key, RqueueWorkerInfo val, Duration duration) {
      lastValue = val;
    }
  }

  private static class TestStringTemplate extends RqueueRedisTemplate<String> {
    private Map<String, String> values = Collections.emptyMap();
    private String lastHashValue;

    TestStringTemplate() {
      super(null);
    }

    @Override
    public Map<String, String> getHashEntries(String key) {
      return values;
    }

    @Override
    public void putHashValue(String key, String hashKey, String val) {
      lastHashValue = val;
    }

    @Override
    public Boolean expire(String key, Duration duration) {
      return true;
    }

    @Override
    public Long deleteHashValues(String key, String... hashKeys) {
      return 0L;
    }
  }
}
