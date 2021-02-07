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

package rqueue.spring.example;

import com.github.sonus21.rqueue.annotation.RqueueListener;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

@Component
@Slf4j
public class MessageListener {
  @RqueueListener(value = "test")
  public void onMessage(String message) {
    log.info("test: {}", message);
  }

  @RqueueListener(value = "dtest")
  public void onMessageQueue(String message) {
    log.info("dtest: {}", message);
  }

  @RqueueListener(value = "job-queue")
  public void onMessageJob(Job job) {
    log.info("job-queue: {}", job);
  }
}
