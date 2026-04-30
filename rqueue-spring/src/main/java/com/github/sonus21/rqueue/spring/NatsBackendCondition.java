/*
 * Copyright (c) 2024-2026 Sonu Kumar
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

import org.springframework.context.annotation.Condition;
import org.springframework.context.annotation.ConditionContext;
import org.springframework.core.type.AnnotatedTypeMetadata;
import org.springframework.util.ClassUtils;

/**
 * Activates the NATS backend wiring when {@code io.nats.client.JetStream} is on the classpath
 * <em>and</em> {@code rqueue.backend=nats} is set in the environment.
 */
public class NatsBackendCondition implements Condition {
  @Override
  public boolean matches(ConditionContext context, AnnotatedTypeMetadata metadata) {
    boolean classPresent =
        ClassUtils.isPresent("io.nats.client.JetStream", context.getClassLoader());
    String backend = context.getEnvironment().getProperty("rqueue.backend");
    return classPresent && "nats".equalsIgnoreCase(backend);
  }
}
