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

package com.github.sonus21.junit;

import java.lang.reflect.AnnotatedElement;
import java.lang.reflect.Method;
import java.nio.charset.StandardCharsets;
import java.util.Optional;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.extension.AfterAllCallback;
import org.junit.jupiter.api.extension.AfterEachCallback;
import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.BeforeEachCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.ExtensionContext.Namespace;
import org.junit.jupiter.api.extension.ExtensionContext.Store;
import org.junit.jupiter.api.extension.ParameterContext;
import org.junit.jupiter.api.extension.ParameterResolutionException;
import org.junit.jupiter.api.extension.ParameterResolver;
import org.springframework.core.annotation.AnnotationUtils;
import org.springframework.data.redis.connection.RedisConnection;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory;
import org.springframework.data.redis.core.RedisConnectionUtils;

@Slf4j
public class RedisBootstrapper extends RedisBootstrapperBase
    implements BeforeAllCallback,
    AfterAllCallback,
    ParameterResolver,
    BeforeEachCallback,
    AfterEachCallback {

  public static final String REDIS_BOOSTRAP_BEAN = "redisBootstrap";

  private static Store getStore(ExtensionContext context) {
    return context.getStore(Namespace.create(REDIS_BOOSTRAP_BEAN));
  }

  @Override
  public void afterAll(ExtensionContext context) throws Exception {
    cleanup();
  }

  @Override
  public void beforeAll(ExtensionContext context) throws Exception {
    Optional<AnnotatedElement> element = context.getElement();
    BootstrapRedis bootstrapRedis =
        AnnotationUtils.findAnnotation(element.get(), BootstrapRedis.class);
    if (bootstrapRedis != null) {
      bootstrap(bootstrapRedis);
      LettuceConnectionFactory redisConnectionFactory =
          new LettuceConnectionFactory(bootstrapRedis.host(), bootstrapRedis.port());
      redisConnectionFactory.afterPropertiesSet();
      getStore(context).put(REDIS_BOOSTRAP_BEAN, redisConnectionFactory);
    }
  }

  @Override
  public boolean supportsParameter(
      ParameterContext parameterContext, ExtensionContext extensionContext)
      throws ParameterResolutionException {
    return parameterContext.getParameter().getType().equals(RedisConnectionFactory.class);
  }

  @Override
  public Object resolveParameter(
      ParameterContext parameterContext, ExtensionContext extensionContext)
      throws ParameterResolutionException {
    return getFactory(extensionContext);
  }

  private RedisConnectionFactory getFactory(ExtensionContext context) {
    LettuceConnectionFactory factory =
        getStore(context).get(REDIS_BOOSTRAP_BEAN, LettuceConnectionFactory.class);
    if (factory == null) {
      throw new ParameterResolutionException("parameter is not available in store");
    }
    return factory;
  }

  private void deleteKeys(ExtensionContext context, TestQueue testQueue) throws Exception {
    byte[][] keys = new byte[testQueue.value().length][];
    for (int i = 0; i < testQueue.value().length; i++) {
      keys[i] = testQueue.value()[i].getBytes(StandardCharsets.UTF_8);
    }
    RedisConnectionFactory factory = getFactory(context);
    RedisConnection connection = RedisConnectionUtils.getConnection(factory);
    connection.del(keys);
    RedisConnectionUtils.releaseConnection(connection, factory);
  }

  private void flushAll(ExtensionContext context) {
    RedisConnectionFactory factory = getFactory(context);
    RedisConnection connection = RedisConnectionUtils.getConnection(factory);
    connection.flushAll();
    RedisConnectionUtils.releaseConnection(connection, factory);
  }

  @Override
  public void afterEach(ExtensionContext context) throws Exception {
    Optional<Method> element = context.getTestMethod();
    TestQueue testQueue = AnnotationUtils.findAnnotation(element.get(), TestQueue.class);
    if (testQueue != null) {
      if (testQueue.clearQueueAfter()) {
        deleteKeys(context, testQueue);
      } else if (testQueue.flushAll()) {
        flushAll(context);
      }
    }
  }

  @Override
  public void beforeEach(ExtensionContext context) throws Exception {
    Optional<Method> element = context.getTestMethod();
    TestQueue testQueue = AnnotationUtils.findAnnotation(element.get(), TestQueue.class);
    log.info("Called {} Ann {}", element, testQueue);

    if (testQueue != null) {
      if (testQueue.clearQueueBefore()) {
        deleteKeys(context, testQueue);
      } else if (testQueue.flushAll()) {
        flushAll(context);
      }
    }
  }
}
