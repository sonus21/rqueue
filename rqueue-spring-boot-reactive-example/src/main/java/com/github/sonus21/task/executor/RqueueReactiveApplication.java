/*
 * Copyright (c) 2019-2023 Sonu Kumar
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

package com.github.sonus21.task.executor;

import com.github.sonus21.rqueue.config.SimpleRqueueListenerContainerFactory;
import com.github.sonus21.rqueue.utils.Constants;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;

@SpringBootApplication
public class RqueueReactiveApplication {

  @Value("${workers.count:3}")
  private int workersCount;

  public static void main(String[] args) {
    SpringApplication.run(RqueueReactiveApplication.class, args);
  }

  @Bean
  public SimpleRqueueListenerContainerFactory simpleRqueueListenerContainerFactory() {
    SimpleRqueueListenerContainerFactory simpleRqueueListenerContainerFactory =
        new SimpleRqueueListenerContainerFactory();
    simpleRqueueListenerContainerFactory.setMaxNumWorkers(workersCount);
    simpleRqueueListenerContainerFactory.setPollingInterval(Constants.ONE_MILLI);
    return simpleRqueueListenerContainerFactory;
  }
}
