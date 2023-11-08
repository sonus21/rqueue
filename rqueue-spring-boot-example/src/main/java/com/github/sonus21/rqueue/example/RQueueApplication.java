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

package com.github.sonus21.rqueue.example;

import com.github.sonus21.rqueue.config.SimpleRqueueListenerContainerFactory;
import com.github.sonus21.rqueue.core.Job;
import com.github.sonus21.rqueue.core.middleware.Middleware;
import com.github.sonus21.rqueue.utils.Constants;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.Callable;
import org.slf4j.MDC;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.util.CollectionUtils;

@SpringBootApplication
public class RQueueApplication {

  @Value("${workers.count:3}")
  private int workersCount;

  public static void main(String[] args) {
    SpringApplication.run(RQueueApplication.class, args);
  }

  static class TraceDataInjector implements Middleware {

    @Override
    public void handle(Job job, Callable<Void> next) throws Exception {
      Object message = job.getMessage();
      if (Objects.nonNull(message) && message instanceof BaseMessage) {
        Map<String, Object> ctx = ((BaseMessage) message).getMetadata();
        if (!CollectionUtils.isEmpty(ctx)) {
          for (Map.Entry<String, Object> e : ctx.entrySet()) {
            MDC.put(e.getKey(), e.getValue().toString());
          }
        }
      }
      next.call();
      MDC.clear();
    }
  }

  @Bean
  public SimpleRqueueListenerContainerFactory simpleRqueueListenerContainerFactory() {
    SimpleRqueueListenerContainerFactory simpleRqueueListenerContainerFactory =
        new SimpleRqueueListenerContainerFactory();
    simpleRqueueListenerContainerFactory.setMaxNumWorkers(workersCount);
    simpleRqueueListenerContainerFactory.setPollingInterval(Constants.ONE_MILLI);
    simpleRqueueListenerContainerFactory.setMiddlewares(Collections.singletonList(new TraceDataInjector()));
    return simpleRqueueListenerContainerFactory;
  }
}
