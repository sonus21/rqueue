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

package com.github.sonus21.rqueue.test.service;

import com.github.sonus21.rqueue.config.RqueueConfig;
import com.github.sonus21.rqueue.core.RqueueEndpointManager;
import com.github.sonus21.rqueue.models.enums.RqueueMode;
import com.github.sonus21.rqueue.models.event.RqueueBootstrapEvent;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.ApplicationListener;
import org.springframework.stereotype.Component;

@Component
@AllArgsConstructor
@Slf4j
public class QueueRegistryUpdater implements ApplicationListener<RqueueBootstrapEvent> {

  private final RqueueEndpointManager rqueueEndpointManager;
  private final RqueueConfig rqueueConfig;

  @Override
  public void onApplicationEvent(RqueueBootstrapEvent event) {
    log.info("Mode {} Event {}", rqueueConfig.getMode(), event);
    if (!RqueueMode.PRODUCER.equals(rqueueConfig.getMode()) || !event.isStartup()) {
      return;
    }
    for (int i = 0; i < 10; i++) {
      String queueName = "new_queue_" + i;
      String[] priorities = null;
      if (i % 3 == 0) {
        priorities = new String[2];
        priorities[0] = "high";
        priorities[1] = "low";
      }
      if (priorities != null) {
        rqueueEndpointManager.registerQueue(queueName, priorities);
      } else {
        rqueueEndpointManager.registerQueue(queueName);
      }
    }
  }
}
