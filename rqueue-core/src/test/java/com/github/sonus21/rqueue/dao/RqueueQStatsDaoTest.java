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
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import com.github.sonus21.TestBase;
import com.github.sonus21.rqueue.CoreUnitTest;
import com.github.sonus21.rqueue.common.RqueueRedisTemplate;
import com.github.sonus21.rqueue.config.RqueueConfig;
import com.github.sonus21.rqueue.dao.impl.RqueueQStatsDaoImpl;
import com.github.sonus21.rqueue.models.db.QueueStatistics;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

@CoreUnitTest
@Slf4j
class RqueueQStatsDaoTest extends TestBase {

  @Mock
  private RqueueRedisTemplate<QueueStatistics> rqueueRedisTemplate;
  @Mock
  private RqueueConfig rqueueConfig;
  private RqueueQStatsDao rqueueQStatsDao;

  @BeforeEach
  public void init() {
    MockitoAnnotations.openMocks(this);
    rqueueQStatsDao = new RqueueQStatsDaoImpl(rqueueRedisTemplate);
  }

  @Test
  void findById() {
    doAnswer(
        invocation -> {
          String name = invocation.getArgument(0);
          return "__rq::q-stat::" + name;
        })
        .when(rqueueConfig)
        .getQueueStatisticsKey(anyString());
    String id = rqueueConfig.getQueueStatisticsKey("job");
    assertNull(rqueueQStatsDao.findById(id));
    QueueStatistics queueStatistics = new QueueStatistics();
    doReturn(queueStatistics).when(rqueueRedisTemplate).get(id);
    assertEquals(queueStatistics, rqueueQStatsDao.findById(id));
    log.info("{}", queueStatistics);
  }

  @Test
  void findAll() {
    doAnswer(
        invocation -> {
          String name = invocation.getArgument(0);
          return "__rq::q-stat::" + name;
        })
        .when(rqueueConfig)
        .getQueueStatisticsKey(anyString());
    List<String> keys =
        Arrays.asList(
            rqueueConfig.getQueueStatisticsKey("job"),
            rqueueConfig.getQueueStatisticsKey("notification"));
    QueueStatistics queueStatistics = new QueueStatistics();
    doReturn(Arrays.asList(null, queueStatistics)).when(rqueueRedisTemplate).mget(keys);
    assertEquals(Collections.singletonList(queueStatistics), rqueueQStatsDao.findAll(keys));
  }

  @Test
  void saveWithoutError() {
    QueueStatistics queueStatistics = new QueueStatistics();
    assertThrows(IllegalArgumentException.class, () -> rqueueQStatsDao.save(queueStatistics));
  }

  @Test
  void saveNull() {
    assertThrows(IllegalArgumentException.class, () -> rqueueQStatsDao.save(null));
  }

  @Test
  void save() {
    doAnswer(
        invocation -> {
          String name = invocation.getArgument(0);
          return "__rq::q-stat::" + name;
        })
        .when(rqueueConfig)
        .getQueueStatisticsKey(anyString());
    QueueStatistics queueStatistics =
        new QueueStatistics(rqueueConfig.getQueueStatisticsKey("job"));
    rqueueQStatsDao.save(queueStatistics);
    verify(rqueueRedisTemplate, times(1)).set(queueStatistics.getId(), queueStatistics);
  }
}
