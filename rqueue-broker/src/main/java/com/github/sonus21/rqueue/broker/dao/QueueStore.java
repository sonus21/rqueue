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

package com.github.sonus21.rqueue.broker.dao;

import com.github.sonus21.rqueue.broker.models.db.QueueConfig;
import com.github.sonus21.rqueue.broker.models.request.BasicQueue;
import com.github.sonus21.rqueue.broker.models.request.Queue;
import com.github.sonus21.rqueue.broker.models.request.QueueWithPriority;
import java.util.List;
import java.util.Map;

public interface QueueStore {

  List<Queue> getAllQueue();

  void addQueue(List<Queue> queues);

  void addConfig(List<QueueConfig> queueConfigs);

  boolean isQueueExist(BasicQueue queue);

  QueueConfig getConfig(QueueWithPriority queue);

  QueueConfig getConfig(BasicQueue queue);

  void update(
      BasicQueue queue,
      QueueConfig queueConfig,
      Long visibilityTimeout,
      Map<String, Integer> userPriority);

  void delete(BasicQueue request);
}
