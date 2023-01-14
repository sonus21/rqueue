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

package com.github.sonus21.rqueue.test.application;

import com.github.sonus21.rqueue.config.SimpleRqueueListenerContainerFactory;
import com.github.sonus21.rqueue.core.Job;
import com.github.sonus21.rqueue.core.context.DefaultContext;
import com.github.sonus21.rqueue.core.middleware.ContextMiddleware;
import com.github.sonus21.rqueue.core.middleware.LoggingMiddleware;
import com.github.sonus21.rqueue.core.middleware.Middleware;
import com.github.sonus21.rqueue.core.middleware.PermissionMiddleware;
import com.github.sonus21.rqueue.core.middleware.RateLimiterMiddleware;
import com.github.sonus21.rqueue.exception.UnknownSwitchCase;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.util.CollectionUtils;
import org.springframework.util.StringUtils;

public abstract class ApplicationWithMiddleware extends BaseApplication {

  @Value("${middleware:}")
  private List<String> middlewareNames;

  @Value("${roles:}")
  private List<String> roles;

  @Value("${rate.limiter.count:5}")
  private int rateLimiterCount;

  @Bean
  public RateLimiter rateLimitingMiddleware() {
    return new RateLimiter(rateLimiterCount);
  }

  @Bean
  public PermissionMiddleware permissionMiddleware() {
    return new TestPermissionMiddleware();
  }

  @Bean
  public ContextMiddleware contextMiddleware() {
    return job -> DefaultContext.withValue(DefaultContext.EMPTY, "UserType", roles);
  }

  @Bean
  public SimpleRqueueListenerContainerFactory simpleRqueueListenerContainerFactory(
      RateLimiter rateLimiter,
      PermissionMiddleware permissionMiddleware,
      ContextMiddleware contextMiddleware) {
    SimpleRqueueListenerContainerFactory factory = new SimpleRqueueListenerContainerFactory();
    factory.setInspectAllBean(false);
    if (StringUtils.isEmpty(middlewareNames)) {
      return factory;
    }
    List<Middleware> middlewares = new ArrayList<>(middlewareNames.size());
    for (String middleware : middlewareNames) {
      switch (middleware) {
        case "log":
          middlewares.add(new LoggingMiddleware());
          break;
        case "context":
          middlewares.add(contextMiddleware);
          break;
        case "rate":
          middlewares.add(rateLimiter);
          break;
        case "permission":
          middlewares.add(permissionMiddleware);
          break;
        default:
          throw new UnknownSwitchCase(middleware);
      }
    }
    if (!CollectionUtils.isEmpty(middlewares)) {
      factory.setMiddlewares(middlewares);
    }
    return factory;
  }

  public static class RateLimiter implements RateLimiterMiddleware {

    private static final Object monitor = new Object();
    private final int maxCount;
    private final List<Job> throttledJobs = new LinkedList<>();
    private int currentCount = 0;

    RateLimiter(int count) {
      this.maxCount = count;
    }

    public List<Job> getThrottledJobs() {
      return throttledJobs;
    }

    @Override
    public boolean isThrottled(Job job) {
      synchronized (monitor) {
        if (currentCount == maxCount) {
          throttledJobs.add(job);
          return true;
        }
        currentCount += 1;
      }
      return false;
    }
  }

  @SuppressWarnings("unchecked")
  public static class TestPermissionMiddleware implements PermissionMiddleware {

    @Override
    public boolean hasPermission(Job job) {
      List<String> roles = (List<String>) job.getContext().getValue("UserRoles");
      if (CollectionUtils.isEmpty(roles)) {
        return false;
      }
      return roles.contains("Admin");
    }
  }
}
