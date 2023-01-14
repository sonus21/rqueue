/*
 * Copyright (c) 2020-2023 Sonu Kumar
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

package com.github.sonus21.junit;

import java.lang.reflect.AnnotatedElement;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;
import org.junit.jupiter.api.extension.ConditionEvaluationResult;
import org.junit.jupiter.api.extension.ExecutionCondition;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.ExtensionContext.Namespace;
import org.junit.jupiter.api.extension.ExtensionContext.Store;
import org.springframework.core.annotation.AnnotationUtils;
import org.springframework.data.redis.connection.RedisNode;

public class RedisAvailableCondition implements ExecutionCondition {

  public static final String REDIS_RUNNING_BEAN = "redisRunning";
  private static final ConditionEvaluationResult ENABLED =
      ConditionEvaluationResult.enabled("@RedisAvailable is not present");

  private RedisRunning createRedisRunning(RedisAvailable redisAvailable) {
    List<RedisNode> redisNodes = new LinkedList<>();
    for (String node : redisAvailable.nodes()) {
      String[] tokens = node.split(":");
      if (tokens.length > 2) {
        throw new IllegalStateException("Redis node is mis-configured");
      }
      String strPort = tokens[1];
      int port;
      String host = "localhost";
      try {
        port = Integer.parseInt(strPort);
      } catch (NumberFormatException e) {
        throw new IllegalStateException(e);
      }
      if (!tokens[0].isEmpty()) {
        host = tokens[0];
      }
      redisNodes.add(new RedisNode(host, port));
    }
    if (redisNodes.isEmpty()) {
      throw new IllegalStateException("RedisAvailable is not configured correctly.");
    }
    return new RedisRunning(redisNodes);
  }

  @Override
  public ConditionEvaluationResult evaluateExecutionCondition(ExtensionContext context) {
    Optional<AnnotatedElement> element = context.getElement();
    RedisAvailable redisAvailable =
        AnnotationUtils.findAnnotation(element.get(), RedisAvailable.class);
    if (redisAvailable != null) {
      try {
        RedisRunning redisRunning = getStore(context).get(REDIS_RUNNING_BEAN, RedisRunning.class);
        if (redisRunning == null) {
          redisRunning = createRedisRunning(redisAvailable);
        }
        redisRunning.isUp();
        Store store = getStore(context);
        store.put(REDIS_RUNNING_BEAN, redisRunning);
        return ConditionEvaluationResult.enabled("Redis is available");
      } catch (Exception e) {
        if (RedisRunning.fatal()) {
          throw new IllegalStateException("Required Redis is not available", e);
        }
        return ConditionEvaluationResult.disabled("Redis is not available");
      }
    }
    return ENABLED;
  }

  private Store getStore(ExtensionContext context) {
    return context.getStore(Namespace.create(getClass(), context));
  }
}
