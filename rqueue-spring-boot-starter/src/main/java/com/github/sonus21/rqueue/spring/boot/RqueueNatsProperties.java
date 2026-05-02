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

import java.time.Duration;
import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;

/** Configuration properties for the optional NATS / JetStream backend. */
@Getter
@Setter
@ConfigurationProperties(prefix = "rqueue.nats")
public class RqueueNatsProperties {

  private Connection connection = new Connection();
  private Stream stream = new Stream();
  private Consumer consumer = new Consumer();
  private Naming naming = new Naming();
  private boolean autoCreateStreams = true;
  private boolean autoCreateConsumers = true;
  private boolean autoCreateDlqStream = false;
  /**
   * When {@code true} (default), each NATS-backed store / dao lazily creates its KV bucket on
   * first use. When {@code false}, {@code NatsKvBucketValidator} verifies at startup that every
   * required bucket already exists and aborts boot with a clear error if any are missing — for
   * deployments where the application credentials lack {@code create} permission on the
   * JetStream account. See the "NATS backend" section in the README for the bucket list and
   * pre-create commands.
   */
  private boolean autoCreateKvBuckets = true;

  @Getter
  @Setter
  public static class Connection {
    private String url;
    private String credentialsPath;
    private String username;
    private String password;
    private String token;
    private boolean tls;
    private String connectionName;
    private Duration connectTimeout;
    private Duration reconnectWait;
    private int maxReconnects = -1;
    private Duration pingInterval;
  }

  @Getter
  @Setter
  public static class Stream {
    private int replicas = 1;
    private String storage = "FILE";
    private String retention = "LIMITS";
    private Duration maxAge = Duration.ofDays(14);
    private long maxBytes = -1;
    private long maxMessages = -1;
    private String discardPolicy = "OLD";
    private Duration duplicateWindow = Duration.ofMinutes(2);
  }

  @Getter
  @Setter
  public static class Consumer {
    private Duration ackWait = Duration.ofSeconds(30);
    private long maxDeliver = 3;
    private long maxAckPending = 1000;
    private Duration fetchWait = Duration.ofSeconds(2);
  }

  @Getter
  @Setter
  public static class Naming {
    private String streamPrefix = "rqueue-js-";
    private String subjectPrefix = "rqueue.js.";
    private String dlqSuffix = "-dlq";
  }
}
