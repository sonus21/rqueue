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

package com.github.sonus21.rqueue.web.dao;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;

import com.github.sonus21.rqueue.common.RqueueRedisTemplate;
import com.github.sonus21.rqueue.models.db.QueueConfig;
import com.github.sonus21.rqueue.utils.QueueUtils;
import com.github.sonus21.rqueue.web.dao.impl.RqueueSystemConfigDaoImpl;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.StrictStubs.class)
public class RqueueSystemConfigDaoTest {
  private RqueueRedisTemplate<QueueConfig> rqueueRedisTemplate = mock(RqueueRedisTemplate.class);
  private RqueueSystemConfigDao rqueueSystemConfigDao =
      new RqueueSystemConfigDaoImpl(rqueueRedisTemplate);

  @Test
  public void getQConfig() {
    assertNull(rqueueSystemConfigDao.getQConfig(QueueUtils.getQueueConfigKey("job")));
    QueueConfig queueConfig =
        new QueueConfig(QueueUtils.getQueueConfigKey("job"), "job", 3, false, 10000L);
    doReturn(queueConfig).when(rqueueRedisTemplate).get(QueueUtils.getQueueConfigKey("job"));
    assertEquals(
        queueConfig, rqueueSystemConfigDao.getQConfig(QueueUtils.getQueueConfigKey("job")));
  }

  @Test
  public void findAllQConfig() {
    assertNull(rqueueSystemConfigDao.getQConfig(QueueUtils.getQueueConfigKey("job")));
    QueueConfig queueConfig =
        new QueueConfig(QueueUtils.getQueueConfigKey("job"), "job", 3, false, 10000L);
    List<String> keys =
        Arrays.asList(
            QueueUtils.getQueueConfigKey("job"), QueueUtils.getQueueConfigKey("notification"));
    doReturn(Arrays.asList(queueConfig, null)).when(rqueueRedisTemplate).mget(keys);
    assertEquals(
        Collections.singletonList(queueConfig), rqueueSystemConfigDao.findAllQConfig(keys));
  }

  @Test
  public void saveAllQConfig() {
    assertNull(rqueueSystemConfigDao.getQConfig(QueueUtils.getQueueConfigKey("job")));
    QueueConfig queueConfig =
        new QueueConfig(QueueUtils.getQueueConfigKey("job"), "job", 3, false, 10000L);
    QueueConfig queueConfig2 =
        new QueueConfig(
            QueueUtils.getQueueConfigKey("notification"), "notification", 3, true, 20000L);
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

  @Test(expected = IllegalArgumentException.class)
  public void saveNullConfig() {
    rqueueSystemConfigDao.saveQConfig(null);
  }

  @Test(expected = IllegalArgumentException.class)
  public void saveIdNullConfig() {
    QueueConfig queueConfig = new QueueConfig();
    rqueueSystemConfigDao.saveQConfig(queueConfig);
  }
}
