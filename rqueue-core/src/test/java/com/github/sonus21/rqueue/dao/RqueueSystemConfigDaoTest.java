/*
 * Copyright 2020 Sonu Kumar
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.github.sonus21.rqueue.dao;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;

import com.github.sonus21.TestBase;
import com.github.sonus21.rqueue.CoreUnitTest;
import com.github.sonus21.rqueue.common.RqueueRedisTemplate;
import com.github.sonus21.rqueue.dao.impl.RqueueSystemConfigDaoImpl;
import com.github.sonus21.rqueue.models.db.QueueConfig;
import com.github.sonus21.rqueue.utils.TestUtils;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

@CoreUnitTest
class RqueueSystemConfigDaoTest extends TestBase {
  private final RqueueRedisTemplate<QueueConfig> rqueueRedisTemplate =
      mock(RqueueRedisTemplate.class);
  private final RqueueSystemConfigDao rqueueSystemConfigDao =
      new RqueueSystemConfigDaoImpl(rqueueRedisTemplate);

  @Test
  void getQConfig() {
    assertNull(rqueueSystemConfigDao.getQConfig(TestUtils.getQueueConfigKey("job")));
    QueueConfig queueConfig = TestUtils.createQueueConfig("job");
    doReturn(queueConfig).when(rqueueRedisTemplate).get(TestUtils.getQueueConfigKey("job"));
    assertEquals(queueConfig, rqueueSystemConfigDao.getQConfig(TestUtils.getQueueConfigKey("job")));
  }

  @Test
  void findAllQConfig() {
    assertNull(rqueueSystemConfigDao.getQConfig(TestUtils.getQueueConfigKey("job")));
    QueueConfig queueConfig = TestUtils.createQueueConfig("job");
    List<String> keys =
        Arrays.asList(
            TestUtils.getQueueConfigKey("job"), TestUtils.getQueueConfigKey("notification"));
    doReturn(Arrays.asList(queueConfig, null)).when(rqueueRedisTemplate).mget(keys);
    assertEquals(
        Collections.singletonList(queueConfig), rqueueSystemConfigDao.findAllQConfig(keys));
  }

  @Test
  void saveAllQConfig() {
    assertNull(rqueueSystemConfigDao.getQConfig(TestUtils.getQueueConfigKey("job")));
    QueueConfig queueConfig = TestUtils.createQueueConfig("job");
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
