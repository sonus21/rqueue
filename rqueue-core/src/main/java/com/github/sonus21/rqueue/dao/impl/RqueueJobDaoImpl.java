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

package com.github.sonus21.rqueue.dao.impl;

import com.github.sonus21.rqueue.common.RqueueRedisTemplate;
import com.github.sonus21.rqueue.config.RqueueConfig;
import com.github.sonus21.rqueue.dao.RqueueJobDao;
import com.github.sonus21.rqueue.dao.RqueueStringDao;
import com.github.sonus21.rqueue.models.db.RqueueJob;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Repository;

@Repository
public class RqueueJobDaoImpl implements RqueueJobDao {

  private final RqueueRedisTemplate<RqueueJob> redisTemplate;
  private final RqueueStringDao rqueueStringDao;
  private final RqueueConfig rqueueConfig;

  @Autowired
  public RqueueJobDaoImpl(RqueueConfig rqueueConfig, RqueueStringDao rqueueStringDao) {
    this.redisTemplate = new RqueueRedisTemplate<>(rqueueConfig.getConnectionFactory());
    this.rqueueStringDao = rqueueStringDao;
    this.rqueueConfig = rqueueConfig;
  }

  @Override
  public void createJob(RqueueJob rqueueJob, Duration expiry) {
    rqueueStringDao.appendToListWithListExpiry(
        rqueueConfig.getJobsKey(rqueueJob.getMessageId()), rqueueJob.getId(), expiry);
    this.save(rqueueJob, expiry);
  }

  @Override
  public void save(RqueueJob rqueueJob, Duration expiry) {
    if (rqueueJob.getCreatedAt() == 0) {
      rqueueJob.setCreatedAt(System.currentTimeMillis());
      rqueueJob.setUpdatedAt(rqueueJob.getCreatedAt());
    } else if (rqueueJob.getUpdatedAt() != 0) {
      rqueueJob.setUpdatedAt(System.currentTimeMillis());
    }
    this.redisTemplate.set(rqueueJob.getId(), rqueueJob, expiry);
  }

  @Override
  public RqueueJob findById(String jobId) {
    return redisTemplate.get(jobId);
  }

  @Override
  public List<RqueueJob> findJobsByIdIn(Collection<String> jobIds) {
    return redisTemplate.mget(jobIds).stream()
        .filter(Objects::nonNull)
        .collect(Collectors.toList());
  }

  @Override
  public List<RqueueJob> finByMessageIdIn(List<String> messageIds) {
    List<String> jobsKeys = new ArrayList<>();
    for (String messageId : messageIds) {
      jobsKeys.add(rqueueConfig.getJobsKey(messageId));
    }
    Map<String, List<Object>> stringListMap = rqueueStringDao.readFromLists(jobsKeys);
    List<String> jobIds = new ArrayList<>();
    for (List<Object> objectJobIds : stringListMap.values()) {
      for (Object objectJobId : objectJobIds) {
        jobIds.add((String) objectJobId);
      }
    }
    return findJobsByIdIn(jobIds);
  }

  @Override
  public List<RqueueJob> finByMessageId(String messageId) {
    return finByMessageIdIn(Collections.singletonList(messageId));
  }

  @Override
  public void delete(String jobId) {
    redisTemplate.delete(jobId);
  }
}
