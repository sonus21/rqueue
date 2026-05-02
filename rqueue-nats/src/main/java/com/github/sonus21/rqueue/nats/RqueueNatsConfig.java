/*
 * Copyright (c) 2026 Sonu Kumar
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * You may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 */
package com.github.sonus21.rqueue.nats;

import io.nats.client.api.RetentionPolicy;
import io.nats.client.api.StorageType;
import java.time.Duration;
import lombok.Getter;
import lombok.Setter;
import lombok.experimental.Accessors;

/**
 * Configuration POJO for the JetStream broker. Use {@link #defaults()} for sensible defaults and
 * the fluent setters to override individual fields. Mutable on purpose - this class is constructed
 * once at startup and read by the broker at runtime; thread-safety is the caller's responsibility.
 */
@Getter
@Setter
@Accessors(chain = true)
public class RqueueNatsConfig {

  private String streamPrefix = "rqueue-js-";
  private String subjectPrefix = "rqueue.js.";
  private String dlqStreamSuffix = "-dlq";
  private String dlqSubjectSuffix = ".dlq";

  private boolean autoCreateStreams = true;
  private boolean autoCreateConsumers = true;
  private boolean autoCreateDlqStream = false;

  private StreamDefaults streamDefaults = new StreamDefaults();
  private ConsumerDefaults consumerDefaults = new ConsumerDefaults();

  /** Default fetch wait when {@code pop()} is called and no explicit wait is given. */
  private Duration defaultFetchWait = Duration.ofSeconds(2);

  public static RqueueNatsConfig defaults() {
    return new RqueueNatsConfig();
  }

  // ---- nested defaults ----------------------------------------------------

  @Getter
  @Setter
  @Accessors(chain = true)
  public static class StreamDefaults {
    private int replicas = 1;
    private StorageType storage = StorageType.File;
    private RetentionPolicy retention = RetentionPolicy.Limits;
    private Duration duplicateWindow = Duration.ofMinutes(2);
    private long maxMsgs = -1;
    private long maxBytes = -1;
    private Duration maxAge = null;
  }

  @Getter
  @Setter
  @Accessors(chain = true)
  public static class ConsumerDefaults {
    private Duration ackWait = Duration.ofSeconds(30);
    private long maxDeliver = 3;
    private long maxAckPending = 1000;
  }
}
