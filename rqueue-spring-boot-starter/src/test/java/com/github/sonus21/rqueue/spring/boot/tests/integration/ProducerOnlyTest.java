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

package com.github.sonus21.rqueue.spring.boot.tests.integration;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.github.sonus21.junit.SpringTestTracerExtension;
import com.github.sonus21.rqueue.core.EndpointRegistry;
import com.github.sonus21.rqueue.spring.boot.application.ProducerOnlyApplication;
import com.github.sonus21.rqueue.test.tests.BasicListenerTest;
import com.github.sonus21.rqueue.utils.PriorityUtils;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestPropertySource;

@ExtendWith(SpringTestTracerExtension.class)
@ContextConfiguration(classes = ProducerOnlyApplication.class)
@SpringBootTest
@Slf4j
@TestPropertySource(properties = {"spring.redis.port=8012", "rqueue.system.mode=PRODUCER"})
public class ProducerOnlyTest extends BasicListenerTest {
  @Test
  public void testQueueCount() {
    assertEquals(18, EndpointRegistry.getRegisteredQueueCount(), rqueueConfig.getMode().toString());
    for (int i = 0; i < 10; i++) {
      String queueName = "new_queue_" + i;
      EndpointRegistry.get(queueName);
      if (i % 3 == 0) {
        EndpointRegistry.get(PriorityUtils.getQueueNameForPriority(queueName, "high"));
        EndpointRegistry.get(PriorityUtils.getQueueNameForPriority(queueName, "low"));
      }
    }
  }
}
