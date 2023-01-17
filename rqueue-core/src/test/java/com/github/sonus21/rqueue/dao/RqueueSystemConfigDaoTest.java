/*
 * Copyright (c) 2020-2023 Sonu Kumar
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

package com.github.sonus21.rqueue.dao;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import com.github.sonus21.TestBase;
import com.github.sonus21.rqueue.CoreUnitTest;
import com.github.sonus21.rqueue.common.RqueueRedisTemplate;
import com.github.sonus21.rqueue.config.RqueueConfig;
import com.github.sonus21.rqueue.dao.impl.RqueueSystemConfigDaoImpl;
import com.github.sonus21.rqueue.models.db.QueueConfig;
import com.github.sonus21.rqueue.utils.TestUtils;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

@CoreUnitTest
class RqueueSystemConfigDaoTest extends TestBase {

  private final String queueName = "job";
  private final String configKey = TestUtils.getQueueConfigKey(queueName);
  private final QueueConfig queueConfig = TestUtils.createQueueConfig(queueName);
  @Mock
  private RqueueRedisTemplate<QueueConfig> rqueueRedisTemplate;
  @Mock
  private RqueueConfig rqueueConfig;
  private RqueueSystemConfigDao rqueueSystemConfigDao;

  @BeforeEach
  public void init() {
    MockitoAnnotations.openMocks(this);
    rqueueSystemConfigDao = new RqueueSystemConfigDaoImpl(rqueueRedisTemplate, rqueueConfig);
  }

  @Test
  void getQConfig() {
    // default return
    assertNull(rqueueSystemConfigDao.getQConfig(TestUtils.getQueueConfigKey(queueName), true));
    doReturn(queueConfig).when(rqueueRedisTemplate).get(TestUtils.getQueueConfigKey(queueName));
    assertEquals(
        queueConfig,
        rqueueSystemConfigDao.getQConfig(TestUtils.getQueueConfigKey(queueName), false));
    assertEquals(
        queueConfig,
        rqueueSystemConfigDao.getQConfig(TestUtils.getQueueConfigKey(queueName), true));
    verify(rqueueRedisTemplate, times(2)).get(any());

    assertEquals(
        queueConfig,
        rqueueSystemConfigDao.getQConfig(TestUtils.getQueueConfigKey(queueName), false));
    verify(rqueueRedisTemplate, times(3)).get(any());
  }

  @Test
  void findAllQConfig() {
    List<String> keys =
        Arrays.asList(
            TestUtils.getQueueConfigKey(queueName), TestUtils.getQueueConfigKey("notification"));
    doReturn(Arrays.asList(queueConfig, null)).when(rqueueRedisTemplate).mget(keys);
    assertEquals(
        Collections.singletonList(queueConfig), rqueueSystemConfigDao.findAllQConfig(keys));
  }

  @Test
  void saveAllQConfig() {
    QueueConfig queueConfig2 = TestUtils.createQueueConfig("notification");
    doAnswer(
        invocation -> {
          Map<String, QueueConfig> configMap = new HashMap<>();
          configMap.put(queueConfig.getId(), queueConfig);
          configMap.put(queueConfig2.getId(), queueConfig2);
          assertEquals(configMap, invocation.getArgument(0));
          return null;
        })
        .when(rqueueRedisTemplate)
        .mset(anyMap());
    rqueueSystemConfigDao.saveAllQConfig(Arrays.asList(queueConfig, queueConfig2));
  }

  @Test
  void getConfigByName() {
    doReturn(queueConfig).when(rqueueRedisTemplate).get(configKey);
    doReturn(configKey).when(rqueueConfig).getQueueConfigKey(queueName);
    QueueConfig config = rqueueSystemConfigDao.getConfigByName(queueName, false);
    assertEquals(queueConfig, config);
    config = rqueueSystemConfigDao.getConfigByName(queueName, true);
    assertEquals(queueConfig, config);
    verify(rqueueRedisTemplate, times(1)).get(any());
  }

  @Test
  void getConfigByNames() {
    doReturn(configKey).when(rqueueConfig).getQueueConfigKey(queueName);
    doReturn(Collections.singletonList(queueConfig))
        .when(rqueueRedisTemplate)
        .mget(Collections.singletonList(configKey));
    List<QueueConfig> configs =
        rqueueSystemConfigDao.getConfigByNames(Collections.singletonList(queueName));
    assertEquals(1, configs.size());
    verify(rqueueRedisTemplate, times(1)).mget(any());
  }

  @Test
  void saveShouldClearCache() {
    doReturn(queueConfig).when(rqueueRedisTemplate).get(configKey);
    rqueueSystemConfigDao.getQConfig(configKey, false);
    QueueConfig updatedConfig = queueConfig.toBuilder().paused(true).build();
    doAnswer(
        invocation -> {
          Map<String, QueueConfig> configMap = new HashMap<>();
          configMap.put(updatedConfig.getId(), updatedConfig);
          assertEquals(configMap, invocation.getArgument(0));
          return null;
        })
        .when(rqueueRedisTemplate)
        .mset(anyMap());
    rqueueSystemConfigDao.saveQConfig(updatedConfig);
    doReturn(updatedConfig).when(rqueueRedisTemplate).get(configKey);
    assertEquals(updatedConfig, rqueueSystemConfigDao.getQConfig(configKey, true));
  }

  @Test
  void saveAllShouldClearCache() {
    doReturn(queueConfig).when(rqueueRedisTemplate).get(configKey);
    rqueueSystemConfigDao.getQConfig(configKey, false);
    QueueConfig updatedConfig = queueConfig.toBuilder().paused(true).build();
    doAnswer(
        invocation -> {
          Map<String, QueueConfig> configMap = new HashMap<>();
          configMap.put(updatedConfig.getId(), updatedConfig);
          assertEquals(configMap, invocation.getArgument(0));
          return null;
        })
        .when(rqueueRedisTemplate)
        .mset(anyMap());
    rqueueSystemConfigDao.saveAllQConfig(Collections.singletonList(updatedConfig));
    doReturn(updatedConfig).when(rqueueRedisTemplate).get(configKey);
    assertEquals(updatedConfig, rqueueSystemConfigDao.getQConfig(configKey, true));
  }

  @Test
  void saveNullConfig() {
    assertThrows(IllegalArgumentException.class, () -> rqueueSystemConfigDao.saveQConfig(null));
  }

  @Test
  void saveIdNullConfig() {
    QueueConfig queueConfig = new QueueConfig();
    assertThrows(
        IllegalArgumentException.class, () -> rqueueSystemConfigDao.saveQConfig(queueConfig));
  }
}
