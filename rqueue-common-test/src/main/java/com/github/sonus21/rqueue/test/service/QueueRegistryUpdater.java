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

package com.github.sonus21.rqueue.test.service;

import com.github.sonus21.rqueue.config.RqueueConfig;
import com.github.sonus21.rqueue.core.RqueueEndpointManager;
import com.github.sonus21.rqueue.core.RqueueMessageSender;
import com.github.sonus21.rqueue.models.enums.RqueueMode;
import javax.annotation.PostConstruct;
import lombok.AllArgsConstructor;
import org.springframework.stereotype.Component;

@Component
@AllArgsConstructor
public class QueueRegistryUpdater {
  private final RqueueMessageSender rqueueMessageSender;
  private final RqueueEndpointManager rqueueEndpointManager;
  private final RqueueConfig rqueueConfig;

  @PostConstruct
  public void registerQueues() {
    if (!RqueueMode.PRODUCER.equals(rqueueConfig.getMode())) {
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
      if (i % 2 == 0) {
        if (priorities != null) {
          rqueueMessageSender.registerQueue(queueName, priorities);
        } else {
          rqueueMessageSender.registerQueue(queueName);
        }
      } else {
        if (priorities != null) {
          rqueueEndpointManager.registerQueue(queueName, priorities);
        } else {
          rqueueEndpointManager.registerQueue(queueName);
        }
      }
    }
  }
}
