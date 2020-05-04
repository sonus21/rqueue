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
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import com.github.sonus21.rqueue.common.RqueueRedisTemplate;
import com.github.sonus21.rqueue.models.db.QueueStatistics;
import com.github.sonus21.rqueue.utils.QueueUtils;
import com.github.sonus21.rqueue.web.dao.impl.RqueueQStatsDaoImpl;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.StrictStubs.class)
@Slf4j
public class RqueueQStatsDaoTest {
  private RqueueRedisTemplate<QueueStatistics> rqueueRedisTemplate =
      mock(RqueueRedisTemplate.class);
  private RqueueQStatsDao rqueueQStatsDao = new RqueueQStatsDaoImpl(rqueueRedisTemplate);

  @Test
  public void findById() {
    String id = QueueUtils.getQueueStatKey("job");
    assertNull(rqueueQStatsDao.findById(id));
    QueueStatistics queueStatistics = new QueueStatistics();
    doReturn(queueStatistics).when(rqueueRedisTemplate).get(id);
    assertEquals(queueStatistics, rqueueQStatsDao.findById(id));
    log.info("{}", queueStatistics);
  }

  @Test
  public void findAll() {
    List<String> keys =
        Arrays.asList(
            QueueUtils.getQueueStatKey("job"), QueueUtils.getQueueStatKey("notification"));
    QueueStatistics queueStatistics = new QueueStatistics();
    doReturn(Arrays.asList(null, queueStatistics)).when(rqueueRedisTemplate).mget(keys);
    assertEquals(Collections.singletonList(queueStatistics), rqueueQStatsDao.findAll(keys));
  }

  @Test(expected = IllegalArgumentException.class)
  public void saveWithoutError() {
    QueueStatistics queueStatistics = new QueueStatistics();
    rqueueQStatsDao.save(queueStatistics);
  }

  @Test(expected = IllegalArgumentException.class)
  public void saveNull() {
    rqueueQStatsDao.save(null);
  }

  @Test
  public void save() {
    QueueStatistics queueStatistics = new QueueStatistics(QueueUtils.getQueueStatKey("job"));
    rqueueQStatsDao.save(queueStatistics);
    verify(rqueueRedisTemplate, times(1)).set(queueStatistics.getId(), queueStatistics);
  }
}
