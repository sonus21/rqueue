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

package com.github.sonus21.rqueue.dao.impl;

import com.github.sonus21.rqueue.common.RqueueRedisTemplate;
import com.github.sonus21.rqueue.config.RqueueConfig;
import com.github.sonus21.rqueue.models.db.RqueueJob;
import com.github.sonus21.rqueue.dao.RqueueJobDao;
import java.time.Duration;
import java.util.Collection;
import java.util.List;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Repository;

@Repository
public class RqueueJobDaoImpl implements RqueueJobDao {
  private final RqueueRedisTemplate<RqueueJob> redisTemplate;

  @Autowired
  public RqueueJobDaoImpl(RqueueConfig rqueueConfig) {
    redisTemplate = new RqueueRedisTemplate<>(rqueueConfig.getConnectionFactory());
  }

  @Override
  public void save(RqueueJob rqueueJob, Duration expiry) {
    this.redisTemplate.set(rqueueJob.getId(), rqueueJob, expiry);
  }

  @Override
  public RqueueJob getJob(String jobId) {
    return redisTemplate.get(jobId);
  }

  @Override
  public List<RqueueJob> getJobs(Collection<String> jobIds) {
    return redisTemplate.mget(jobIds);
  }
}
