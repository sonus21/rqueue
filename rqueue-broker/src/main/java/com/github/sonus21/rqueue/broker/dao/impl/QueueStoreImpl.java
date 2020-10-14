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

package com.github.sonus21.rqueue.broker.dao.impl;

import com.github.sonus21.rqueue.broker.dao.QueueStore;
import com.github.sonus21.rqueue.broker.models.db.QueueConfig;
import com.github.sonus21.rqueue.broker.models.request.BasicQueue;
import com.github.sonus21.rqueue.broker.models.request.Queue;
import com.github.sonus21.rqueue.broker.models.request.QueueWithPriority;
import java.util.List;
import java.util.Map;
import org.springframework.stereotype.Repository;

@Repository
public class QueueStoreImpl implements QueueStore {

  @Override
  public List<Queue> getAllQueue() {
    return null;
  }

  @Override
  public void addQueue(List<Queue> queues) {

  }

  @Override
  public void addConfig(
      List<QueueConfig> queueConfigs) {

  }

  @Override
  public boolean isQueueExist(BasicQueue queue) {
    return false;
  }

  @Override
  public QueueConfig getConfig(
      QueueWithPriority queue) {
    return null;
  }

  @Override
  public QueueConfig getConfig(BasicQueue queue) {
    return null;
  }

  @Override
  public void update(BasicQueue queue,
      QueueConfig queueConfig, Long visibilityTimeout,
      Map<String, Integer> userPriority) {

  }

  @Override
  public void delete(BasicQueue request) {

  }
}
