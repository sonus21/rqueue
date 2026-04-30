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

package com.github.sonus21.rqueue.listener;

import com.github.sonus21.rqueue.annotation.RqueueListener;

/**
 * Resolves the per-listener durable consumer name used by capability-gated backends (currently
 * NATS / JetStream).
 *
 * <p>Additive helper introduced in Phase 3. The Redis backend never invokes this; it's only used
 * by the listener container when the active {@code MessageBroker} reports
 * {@code usesPrimaryHandlerDispatch == false}.
 */
public final class ConsumerNameResolver {

  private ConsumerNameResolver() {}

  /**
   * @param annotation the {@link RqueueListener} on the target method
   * @param beanName Spring bean name owning the method
   * @param methodName the listener method's simple name
   * @param queueName the resolved queue name
   * @return explicit {@code consumerName()} when set, else
   *     {@code "rqueue-<queue>-<bean>_<method>"} with bean/method sanitized to
   *     {@code [A-Za-z0-9_-]} (NATS / JetStream's allowed character set for durable consumer
   *     names; nested-class beans carry {@code $} which would otherwise be rejected).
   */
  public static String resolveConsumerName(
      RqueueListener annotation, String beanName, String methodName, String queueName) {
    if (annotation != null
        && annotation.consumerName() != null
        && !annotation.consumerName().isEmpty()) {
      return annotation.consumerName();
    }
    String safeBean = sanitize(beanName);
    String safeMethod = sanitize(methodName);
    return "rqueue-" + queueName + "-" + safeBean + "_" + safeMethod;
  }

  /** Restrict to [A-Za-z0-9_-]; collapse any other character to '_'. */
  private static String sanitize(String s) {
    if (s == null || s.isEmpty()) {
      return "_";
    }
    return s.replaceAll("[^A-Za-z0-9_-]", "_");
  }
}
