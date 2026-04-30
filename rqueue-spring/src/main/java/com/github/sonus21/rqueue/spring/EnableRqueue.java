/*
 * Copyright (c) 2019-2026 Sonu Kumar
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

import com.github.sonus21.rqueue.config.Backend;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import org.springframework.context.annotation.Import;

/**
 * Auto-configure Rqueue when {@link
 * org.springframework.data.redis.connection.RedisConnectionFactory} (or, when {@link
 * #backend()} is {@link Backend#NATS}, an {@link io.nats.client.Connection}-derived
 * {@code MessageBroker}) is available. All other beans are created automatically; further
 * customization happens through {@link
 * com.github.sonus21.rqueue.config.SimpleRqueueListenerContainerFactory} or by directly defining
 * the beans created in {@link RqueueListenerConfig}.
 */
@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
@Import({RqueueBackendImportSelector.class})
public @interface EnableRqueue {

  /**
   * Backend to wire. Defaults to {@link Backend#REDIS}. Set to {@link Backend#NATS} to import the
   * NATS / JetStream listener configuration. The {@code rqueue.backend} property is independently
   * read by {@link com.github.sonus21.rqueue.config.RqueueConfig} and remains the source of truth
   * for the runtime backend; this attribute controls only which {@code @Configuration} class is
   * pulled into the context.
   */
  Backend backend() default Backend.REDIS;
}
