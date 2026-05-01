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

import com.github.sonus21.rqueue.core.spi.MessageBroker;
import com.github.sonus21.rqueue.metrics.RqueueQueueMetricsProvider;
import com.github.sonus21.rqueue.nats.RqueueNatsConfig;
import com.github.sonus21.rqueue.nats.js.JetStreamMessageBroker;
import com.github.sonus21.rqueue.nats.js.NatsStreamValidator;
import com.github.sonus21.rqueue.nats.kv.NatsKvBucketValidator;
import com.github.sonus21.rqueue.nats.metrics.NatsRqueueQueueMetricsProvider;
import io.nats.client.Connection;
import io.nats.client.JetStream;
import io.nats.client.JetStreamManagement;
import io.nats.client.Nats;
import io.nats.client.Options;
import java.io.IOException;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.AutoConfigureBefore;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.DependsOn;

/**
 * Auto-configuration that wires a JetStream-backed {@link MessageBroker} when
 * {@code rqueue.backend=nats} and the jnats client is on the classpath.
 *
 * <p>This auto-config runs before {@link RqueueListenerAutoConfig} so that the broker bean is
 * available for the listener container factory to consume; the existing Redis broker bean uses
 * {@code @ConditionalOnMissingBean(MessageBroker.class)} so it backs off when this one is present.
 */
@AutoConfiguration
@AutoConfigureBefore(RqueueListenerAutoConfig.class)
@ConditionalOnClass(JetStream.class)
@ConditionalOnProperty(name = "rqueue.backend", havingValue = "nats")
@EnableConfigurationProperties(RqueueNatsProperties.class)
public class RqueueNatsAutoConfig {

  @Bean
  @ConditionalOnMissingBean
  public Connection natsConnection(RqueueNatsProperties props) throws IOException {
    Options.Builder ob = new Options.Builder();
    RqueueNatsProperties.Connection c = props.getConnection();
    if (c.getUrls() != null && !c.getUrls().isEmpty()) {
      ob.servers(c.getUrls().toArray(new String[0]));
    } else if (c.getUrl() != null && !c.getUrl().isEmpty()) {
      ob.server(c.getUrl());
    } else {
      ob.server(Options.DEFAULT_URL);
    }
    if (c.getConnectionName() != null) {
      ob.connectionName(c.getConnectionName());
    }
    if (c.getToken() != null && !c.getToken().isEmpty()) {
      ob.token(c.getToken().toCharArray());
    } else if (c.getUsername() != null && c.getPassword() != null) {
      ob.userInfo(c.getUsername(), c.getPassword());
    }
    if (c.getConnectTimeout() != null) {
      ob.connectionTimeout(c.getConnectTimeout());
    }
    if (c.getReconnectWait() != null) {
      ob.reconnectWait(c.getReconnectWait());
    }
    if (c.getMaxReconnects() >= 0) {
      ob.maxReconnects(c.getMaxReconnects());
    }
    if (c.getPingInterval() != null) {
      ob.pingInterval(c.getPingInterval());
    }
    Connection connection;
    try {
      connection = Nats.connect(ob.build());
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new IOException("Interrupted while connecting to NATS", e);
    }
    // Validate KV buckets here — inside the Connection bean factory — so every other NATS
    // bean (broker, daos, registry, lock manager) that injects Connection is guaranteed to
    // see a validated cluster. Spring's @Order/@Priority don't control bean creation order;
    // anchoring the check on the Connection itself does.
    NatsKvBucketValidator.validate(connection, props.isAutoCreateKvBuckets());
    return connection;
  }

  @Bean
  @ConditionalOnMissingBean
  public JetStream jetStream(Connection connection) throws IOException {
    return connection.jetStream();
  }

  @Bean
  @ConditionalOnMissingBean
  public JetStreamManagement jetStreamManagement(Connection connection) throws IOException {
    return connection.jetStreamManagement();
  }

  @Bean
  @ConditionalOnMissingBean(MessageBroker.class)
  public MessageBroker jetStreamMessageBroker(
      Connection connection,
      JetStream jetStream,
      JetStreamManagement jetStreamManagement,
      RqueueNatsProperties props) {
    return JetStreamMessageBroker.builder()
        .connection(connection)
        .jetStream(jetStream)
        .management(jetStreamManagement)
        .config(toBrokerConfig(props))
        .build();
  }

  @Bean
  @ConditionalOnMissingBean(RqueueQueueMetricsProvider.class)
  public RqueueQueueMetricsProvider natsRqueueQueueMetricsProvider(
      JetStreamManagement jetStreamManagement, RqueueNatsProperties props) {
    return new NatsRqueueQueueMetricsProvider(jetStreamManagement, toBrokerConfig(props));
  }

  /**
   * Boot-time stream / DLQ existence guard. Fires on {@code RqueueBootstrapEvent} so it sees the
   * full {@code EndpointRegistry} after every {@code @RqueueListener} has registered. Removes
   * the per-publish {@code getStreamInfo} round-trip from the broker hot path.
   */
  @Bean
  @ConditionalOnMissingBean(NatsStreamValidator.class)
  public NatsStreamValidator natsStreamValidator(
      JetStreamManagement jetStreamManagement, RqueueNatsProperties props) {
    return new NatsStreamValidator(jetStreamManagement, toBrokerConfig(props));
  }

  /**
   * Bean form of the KV-bucket validator. Other NATS beans {@code @DependsOn} this name so it
   * runs before they are constructed. The flag is sourced from {@link RqueueNatsProperties} —
   * {@code rqueue-nats} itself never reads {@code rqueue.nats.*} keys directly.
   */
  @Bean
  @ConditionalOnMissingBean(NatsKvBucketValidator.class)
  public NatsKvBucketValidator natsKvBucketValidator(
      Connection connection, RqueueNatsProperties props) {
    return new NatsKvBucketValidator(connection, props.isAutoCreateKvBuckets());
  }

  @Bean
  @ConditionalOnMissingBean(com.github.sonus21.rqueue.worker.WorkerRegistryStore.class)
  @DependsOn("natsKvBucketValidator")
  public com.github.sonus21.rqueue.worker.WorkerRegistryStore natsWorkerRegistryStore(
      Connection connection) throws IOException {
    return new com.github.sonus21.rqueue.nats.worker.NatsWorkerRegistryStore(connection);
  }

  @Bean
  @ConditionalOnBean(com.github.sonus21.rqueue.config.RqueueConfig.class)
  @ConditionalOnMissingBean(com.github.sonus21.rqueue.worker.RqueueWorkerRegistry.class)
  public com.github.sonus21.rqueue.worker.RqueueWorkerRegistry natsRqueueWorkerRegistry(
      com.github.sonus21.rqueue.config.RqueueConfig rqueueConfig,
      com.github.sonus21.rqueue.worker.WorkerRegistryStore workerRegistryStore) {
    return new com.github.sonus21.rqueue.worker.RqueueWorkerRegistryImpl(
        rqueueConfig, workerRegistryStore);
  }

  /**
   * NATS-side {@link com.github.sonus21.rqueue.repository.MessageBrowsingRepository} powering
   * the dashboard's data-explorer panel. JetStream KV doesn't model arbitrary keyed reads, so
   * this impl returns 0 sizes and throws {@code BackendCapabilityException} from
   * {@code viewData} (mapped to HTTP 501 by {@code RqueueWebExceptionAdvice}).
   */
  @Bean
  @ConditionalOnMissingBean(com.github.sonus21.rqueue.repository.MessageBrowsingRepository.class)
  public com.github.sonus21.rqueue.repository.MessageBrowsingRepository
      natsMessageBrowsingRepository() {
    return new com.github.sonus21.rqueue.nats.repository.NatsMessageBrowsingRepository();
  }

  static RqueueNatsConfig toBrokerConfig(RqueueNatsProperties p) {
    RqueueNatsConfig cfg = RqueueNatsConfig.defaults();
    cfg.setStreamPrefix(p.getNaming().getStreamPrefix());
    cfg.setSubjectPrefix(p.getNaming().getSubjectPrefix());
    cfg.setDlqStreamSuffix(p.getNaming().getDlqSuffix());
    cfg.setAutoCreateStreams(p.isAutoCreateStreams());
    cfg.setAutoCreateConsumers(p.isAutoCreateConsumers());
    cfg.setAutoCreateDlqStream(p.isAutoCreateDlqStream());
    cfg.setDefaultFetchWait(p.getConsumer().getFetchWait());

    RqueueNatsConfig.StreamDefaults sd = new RqueueNatsConfig.StreamDefaults();
    sd.setReplicas(p.getStream().getReplicas());
    if ("MEMORY".equalsIgnoreCase(p.getStream().getStorage())) {
      sd.setStorage(io.nats.client.api.StorageType.Memory);
    } else {
      sd.setStorage(io.nats.client.api.StorageType.File);
    }
    sd.setMaxMsgs(p.getStream().getMaxMessages());
    sd.setMaxBytes(p.getStream().getMaxBytes());
    if (p.getStream().getDuplicateWindow() != null) {
      sd.setDuplicateWindow(p.getStream().getDuplicateWindow());
    }
    cfg.setStreamDefaults(sd);

    RqueueNatsConfig.ConsumerDefaults cd = new RqueueNatsConfig.ConsumerDefaults();
    cd.setAckWait(p.getConsumer().getAckWait());
    cd.setMaxDeliver(p.getConsumer().getMaxDeliver());
    cd.setMaxAckPending(p.getConsumer().getMaxAckPending());
    cfg.setConsumerDefaults(cd);
    return cfg;
  }
}
