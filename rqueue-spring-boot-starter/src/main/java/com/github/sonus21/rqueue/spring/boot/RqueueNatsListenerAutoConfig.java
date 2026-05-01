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
package com.github.sonus21.rqueue.spring.boot;

import com.github.sonus21.rqueue.config.RqueueConfig;
import com.github.sonus21.rqueue.nats.kv.NatsKvBucketValidator;
import com.github.sonus21.rqueue.nats.worker.NatsWorkerRegistryStore;
import com.github.sonus21.rqueue.worker.RqueueWorkerRegistry;
import com.github.sonus21.rqueue.worker.RqueueWorkerRegistryImpl;
import com.github.sonus21.rqueue.worker.WorkerRegistryStore;
import io.nats.client.Connection;
import io.nats.client.JetStream;
import java.io.IOException;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.AutoConfigureAfter;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.DependsOn;

/**
 * Post-listener auto-configuration that wires the NATS-backed worker registry after
 * {@link RqueueListenerAutoConfig} has run and the {@link RqueueConfig} bean is available.
 *
 * <p>{@link RqueueNatsAutoConfig} must run <em>before</em> the listener config so it can supply
 * the {@link com.github.sonus21.rqueue.core.spi.MessageBroker} bean in time. However, the worker
 * registry depends on {@link RqueueConfig}, which is only created by
 * {@link RqueueListenerAutoConfig}. Splitting these two concerns into two auto-configs — one
 * before and one after — breaks the ordering deadlock that caused
 * {@code @ConditionalOnBean(RqueueConfig.class)} to always evaluate false.
 */
@AutoConfiguration
@AutoConfigureAfter(RqueueListenerAutoConfig.class)
@ConditionalOnClass(JetStream.class)
@ConditionalOnProperty(name = "rqueue.backend", havingValue = "nats")
public class RqueueNatsListenerAutoConfig {

  /**
   * NATS KV-backed store that persists worker registration entries.
   *
   * <p>{@code @DependsOn("natsKvBucketValidator")} ensures the KV bucket exists before the store
   * tries to bind to it.
   */
  @Bean
  @ConditionalOnMissingBean(WorkerRegistryStore.class)
  @DependsOn("natsKvBucketValidator")
  public WorkerRegistryStore natsWorkerRegistryStore(Connection connection) throws IOException {
    return new NatsWorkerRegistryStore(connection);
  }

  /**
   * Worker registry backed by NATS KV. {@link RqueueConfig} is guaranteed to be present here
   * because this auto-config runs after {@link RqueueListenerAutoConfig}.
   */
  @Bean
  @ConditionalOnMissingBean(RqueueWorkerRegistry.class)
  public RqueueWorkerRegistry natsRqueueWorkerRegistry(
      RqueueConfig rqueueConfig, WorkerRegistryStore workerRegistryStore) {
    return new RqueueWorkerRegistryImpl(rqueueConfig, workerRegistryStore);
  }

  /**
   * Guard bean that validates KV buckets. Defined here so it is available for
   * {@code @DependsOn("natsKvBucketValidator")} references within this config even when the
   * primary definition in {@link RqueueNatsAutoConfig} was conditionally skipped.
   *
   * <p>In practice the primary definition always wins; this is a fallback safety net.
   */
  @Bean
  @ConditionalOnMissingBean(NatsKvBucketValidator.class)
  public NatsKvBucketValidator natsKvBucketValidatorFallback(
      Connection connection, RqueueNatsProperties props) {
    return new NatsKvBucketValidator(connection, props.isAutoCreateKvBuckets());
  }
}
