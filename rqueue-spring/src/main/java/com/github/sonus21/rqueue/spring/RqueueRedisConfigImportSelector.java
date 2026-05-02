/*
 * Copyright (c) 2026 Sonu Kumar
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
package com.github.sonus21.rqueue.spring;

import org.springframework.context.annotation.ImportSelector;
import org.springframework.core.type.AnnotationMetadata;
import org.springframework.util.ClassUtils;

/**
 * Conditionally imports {@code RqueueRedisListenerConfig} only when the {@code rqueue-redis} module
 * is on the classpath. A direct {@code @Import(RqueueRedisListenerConfig.class)} would cause Spring
 * to read the class bytecode before any {@code @Conditional} can fire, throwing
 * {@code FileNotFoundException} when the module is excluded.
 */
public class RqueueRedisConfigImportSelector implements ImportSelector {

  private static final String REDIS_CONFIG_CLASS =
      "com.github.sonus21.rqueue.redis.config.RqueueRedisListenerConfig";

  @Override
  public String[] selectImports(AnnotationMetadata importingClassMetadata) {
    if (ClassUtils.isPresent(REDIS_CONFIG_CLASS, getClass().getClassLoader())) {
      return new String[] {REDIS_CONFIG_CLASS};
    }
    return new String[0];
  }
}
