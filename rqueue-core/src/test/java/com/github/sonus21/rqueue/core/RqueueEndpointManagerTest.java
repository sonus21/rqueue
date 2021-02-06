/*
 *  Copyright 2021 Sonu Kumar
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *         https://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and limitations under the License.
 *
 */

package com.github.sonus21.rqueue.core;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;

import com.github.sonus21.TestBase;
import com.github.sonus21.rqueue.CoreUnitTest;
import com.github.sonus21.rqueue.config.RqueueConfig;
import com.github.sonus21.rqueue.core.impl.RqueueEndpointManagerImpl;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

@CoreUnitTest
class RqueueEndpointManagerTest extends TestBase {

  private final RqueueEndpointManager rqueueEndpointManager =
      new RqueueEndpointManagerImpl(
          mock(RqueueMessageTemplate.class), new DefaultRqueueMessageConverter(), null);
  RqueueConfig rqueueConfig = mock(RqueueConfig.class);

  @BeforeEach
  public void init() throws IllegalAccessException {
    FieldUtils.writeField(rqueueEndpointManager, "rqueueConfig", rqueueConfig, true);
    EndpointRegistry.delete();
  }

  @AfterEach
  public void clean() {
    EndpointRegistry.delete();
  }

  @Test
  void registerQueue() {
    rqueueEndpointManager.registerQueue("test", "high");
    rqueueEndpointManager.isQueueRegistered("test");
    rqueueEndpointManager.isQueueRegistered("test", "high");
  }

  @Test
  void getQueueConfig() {
    rqueueEndpointManager.registerQueue("test2", "high");
    assertEquals(2, rqueueEndpointManager.getQueueConfig("test2").size());
  }
}
