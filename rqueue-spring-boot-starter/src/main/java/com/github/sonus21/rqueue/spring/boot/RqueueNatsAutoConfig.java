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
import com.github.sonus21.rqueue.nats.internal.NatsProvisioner;
import com.github.sonus21.rqueue.nats.js.JetStreamMessageBroker;
import com.github.sonus21.rqueue.nats.js.NatsStreamValidator;
import com.github.sonus21.rqueue.nats.kv.NatsKvBucketValidator;
import com.github.sonus21.rqueue.nats.metrics.NatsRqueueQueueMetricsProvider;
import com.github.sonus21.rqueue.serdes.RqJacksonSerDes;
import com.github.sonus21.rqueue.serdes.SerializationUtils;
import com.github.sonus21.rqueue.utils.StringUtils;
import io.nats.client.Connection;
import io.nats.client.JetStream;
import io.nats.client.JetStreamManagement;
import io.nats.client.Nats;
import io.nats.client.Options;
import java.io.IOException;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.AutoConfigureBefore;
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
    if (!StringUtils.isEmpty(c.getUrl())) {
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
      NatsProvisioner natsProvisioner,
      RqueueNatsProperties props) {
    return JetStreamMessageBroker.builder()
        .connection(connection)
        .jetStream(jetStream)
        .management(jetStreamManagement)
        .config(toBrokerConfig(props))
        .provisioner(natsProvisioner)
        .build();
  }

  @Bean
  @ConditionalOnMissingBean(RqueueQueueMetricsProvider.class)
  public RqueueQueueMetricsProvider natsRqueueQueueMetricsProvider(
      JetStreamManagement jetStreamManagement, RqueueNatsProperties props) {
    return new NatsRqueueQueueMetricsProvider(jetStreamManagement, toBrokerConfig(props));
  }

  /**
   * Boot-time stream / DLQ existence guard. Implements {@code SmartInitializingSingleton} so it
   * runs after every {@code @RqueueListener} has registered with {@code EndpointRegistry} but
   * before {@code SmartLifecycle.start()} spawns the message pollers — otherwise pollers race
   * the validator and surface {@code stream not found [10059]}. Removes the per-publish
   * {@code getStreamInfo} round-trip from the broker hot path.
   */
  @Bean
  @ConditionalOnMissingBean(NatsStreamValidator.class)
  public NatsStreamValidator natsStreamValidator(
      NatsProvisioner natsProvisioner, RqueueNatsProperties props) {
    return new NatsStreamValidator(natsProvisioner, toBrokerConfig(props));
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

  /**
   * Shared {@link com.github.sonus21.rqueue.serdes.RqueueSerDes} for the NATS backend. Backed by Jackson with the same configuration as
   * the rest of rqueue ({@code FAIL_ON_UNKNOWN_PROPERTIES=false}, auto-detected modules) so values
   * written to KV buckets are readable via {@code nats kv get}.
   */
  @Bean
  @ConditionalOnMissingBean(com.github.sonus21.rqueue.serdes.RqueueSerDes.class)
  public com.github.sonus21.rqueue.serdes.RqueueSerDes natsSerDes() {
    return new RqJacksonSerDes(SerializationUtils.getObjectMapper());
  }

  @Bean
  @ConditionalOnMissingBean(NatsProvisioner.class)
  @DependsOn("natsKvBucketValidator")
  public NatsProvisioner natsProvisioner(
      Connection connection, JetStreamManagement jetStreamManagement, RqueueNatsProperties props)
      throws IOException {
    return new NatsProvisioner(connection, jetStreamManagement, toBrokerConfig(props));
  }

  /**
   * NATS-side {@link com.github.sonus21.rqueue.repository.MessageBrowsingRepository} powering
   * the dashboard's data-explorer and queue-detail panels. Maps Redis-style queue names to
   * JetStream streams and returns actual message counts from the broker. JetStream KV doesn't
   * model arbitrary keyed reads, so {@link #viewData} throws {@code BackendCapabilityException}
   * (mapped to HTTP 501 by {@code RqueueWebExceptionAdvice}).
   */
  @Bean
  @ConditionalOnMissingBean(com.github.sonus21.rqueue.repository.MessageBrowsingRepository.class)
  public com.github.sonus21.rqueue.repository.MessageBrowsingRepository
      natsMessageBrowsingRepository(
          JetStreamManagement jetStreamManagement, RqueueNatsProperties props) {
    return new com.github.sonus21.rqueue.nats.repository.NatsMessageBrowsingRepository(
        jetStreamManagement, toBrokerConfig(props));
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
    sd.setStorage(
        "MEMORY".equalsIgnoreCase(p.getStream().getStorage())
            ? io.nats.client.api.StorageType.Memory
            : io.nats.client.api.StorageType.File);
    sd.setRetention(
        "WORKQUEUE".equalsIgnoreCase(p.getStream().getRetention())
            ? io.nats.client.api.RetentionPolicy.WorkQueue
            : "INTEREST".equalsIgnoreCase(p.getStream().getRetention())
                ? io.nats.client.api.RetentionPolicy.Interest
                : io.nats.client.api.RetentionPolicy.Limits);
    sd.setMaxMsgs(p.getStream().getMaxMessages());
    sd.setMaxBytes(p.getStream().getMaxBytes());
    sd.setMaxAge(p.getStream().getMaxAge());
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
