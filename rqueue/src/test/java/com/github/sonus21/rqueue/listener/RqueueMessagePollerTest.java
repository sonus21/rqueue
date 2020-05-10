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

package com.github.sonus21.rqueue.listener;

import com.github.sonus21.rqueue.utils.backoff.TaskExecutionBackOff;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.junit.Test;

public class RqueueMessagePollerTest {

  @Test
  public void getPollingInterval() {}

  @Test
  public void getBackOffTime() {}

  @Test
  public void shouldExit() {}

  @Test
  public void poll() {}

  @Test
  public void getSemaphoreWaiTime() {}

  @Test
  public void deactivate() {}

  class TestRqueueMessagePoller extends RqueueMessagePoller {
    private Map<String, Integer> deactivationRequest;

    TestRqueueMessagePoller(
        String groupName,
        RqueueMessageListenerContainer container,
        TaskExecutionBackOff taskExecutionBackOff,
        int retryPerPoll) {
      super(groupName, container, taskExecutionBackOff, retryPerPoll);
      this.deactivationRequest = new HashMap<>();
    }

    @Override
    long getSemaphoreWaiTime() {
      return 10;
    }

    @Override
    void deactivate(int index, String queue, DeactivateType deactivateType) {}

    @Override
    void start() {}
  }
}
