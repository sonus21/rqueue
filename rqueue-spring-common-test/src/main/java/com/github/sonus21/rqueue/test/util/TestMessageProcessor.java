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

package com.github.sonus21.rqueue.test.util;

import com.github.sonus21.rqueue.core.Job;
import com.github.sonus21.rqueue.core.RqueueMessage;
import com.github.sonus21.rqueue.core.support.MessageProcessor;
import java.util.List;
import java.util.Vector;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

@Getter
@Slf4j
public class TestMessageProcessor implements MessageProcessor {

  private final String name;
  private final List<RqueueMessageEnvelop> rqueueMessageList = new Vector<>();

  public TestMessageProcessor(String name) {
    this.name = name;
  }

  public void clear() {
    this.rqueueMessageList.clear();
  }

  public int count() {
    return rqueueMessageList.size();
  }

  @Override
  public boolean process(Job job) {
    log.info("{}MessageProcessor called queued {} with {}", name,
        job.getRqueueMessage().getQueueName(), job.getRqueueMessage());
    rqueueMessageList.add(
        new RqueueMessageEnvelop(job.getRqueueMessage(), System.currentTimeMillis()));
    return true;
  }

  @Getter
  @Setter
  @AllArgsConstructor
  public static class RqueueMessageEnvelop {

    private RqueueMessage message;
    private Long createdAt;
  }
}
