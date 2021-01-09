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

package com.github.sonus21.rqueue.test.application;

import com.github.sonus21.rqueue.config.SimpleRqueueListenerContainerFactory;
import com.github.sonus21.rqueue.core.support.Middleware;
import com.github.sonus21.rqueue.test.middlewares.LoggingMiddleware;
import com.github.sonus21.rqueue.test.middlewares.RateLimitingMiddleware;
import java.util.ArrayList;
import java.util.List;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.util.CollectionUtils;
import org.springframework.util.StringUtils;

public abstract class ApplicationWithContainerFactory extends BaseApplication {

  @Value("${middleware:}")
  private List<String> middlewareNames;

  @Value("${rate.limiter.count:5}")
  private int rateLimiterCount;

  @Bean
  public SimpleRqueueListenerContainerFactory simpleRqueueListenerContainerFactory() {
    SimpleRqueueListenerContainerFactory factory = new SimpleRqueueListenerContainerFactory();
    factory.setInspectAllBean(false);
    if (StringUtils.isEmpty(middlewareNames)) {
      return factory;
    }
    List<Middleware> middlewares = new ArrayList<>(middlewareNames.size());
    for (String middleware : middlewareNames) {
      if (middleware.equals("log")) {
        middlewares.add(new LoggingMiddleware());
      } else if (middleware.equals("rate")) {
        middlewares.add(new RateLimitingMiddleware(rateLimiterCount));
      }
    }
    if (!CollectionUtils.isEmpty(middlewares)) {
      factory.setMiddlewares(middlewares);
    }
    return factory;
  }
}
