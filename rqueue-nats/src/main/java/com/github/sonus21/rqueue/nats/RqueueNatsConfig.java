/*
 * Copyright (c) 2024-2026 Sonu Kumar
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

/**
 * Configuration POJO for the JetStream broker. Use {@link #defaults()} for sensible defaults and
 * the fluent setters to override individual fields. Mutable on purpose - this class is constructed
 * once at startup and read by the broker at runtime; thread-safety is the caller's responsibility.
 */
public class RqueueNatsConfig {

  private String streamPrefix = "rqueue-";
  private String subjectPrefix = "rqueue.";
  private String dlqStreamSuffix = "-dlq";
  private String dlqSubjectSuffix = ".dlq";

  private boolean autoCreateStreams = true;
  private boolean autoCreateConsumers = true;
  private boolean autoCreateDlqStream = true;

  private StreamDefaults streamDefaults = new StreamDefaults();
  private ConsumerDefaults consumerDefaults = new ConsumerDefaults();

  /** Default fetch wait when {@code pop()} is called and no explicit wait is given. */
  private Duration defaultFetchWait = Duration.ofSeconds(2);

  public static RqueueNatsConfig defaults() {
    return new RqueueNatsConfig();
  }

  // ---- getters / setters --------------------------------------------------

  public String getStreamPrefix() {
    return streamPrefix;
  }

  public RqueueNatsConfig setStreamPrefix(String streamPrefix) {
    this.streamPrefix = streamPrefix;
    return this;
  }

  public String getSubjectPrefix() {
    return subjectPrefix;
  }

  public RqueueNatsConfig setSubjectPrefix(String subjectPrefix) {
    this.subjectPrefix = subjectPrefix;
    return this;
  }

  public String getDlqStreamSuffix() {
    return dlqStreamSuffix;
  }

  public RqueueNatsConfig setDlqStreamSuffix(String dlqStreamSuffix) {
    this.dlqStreamSuffix = dlqStreamSuffix;
    return this;
  }

  public String getDlqSubjectSuffix() {
    return dlqSubjectSuffix;
  }

  public RqueueNatsConfig setDlqSubjectSuffix(String dlqSubjectSuffix) {
    this.dlqSubjectSuffix = dlqSubjectSuffix;
    return this;
  }

  public boolean isAutoCreateStreams() {
    return autoCreateStreams;
  }

  public RqueueNatsConfig setAutoCreateStreams(boolean autoCreateStreams) {
    this.autoCreateStreams = autoCreateStreams;
    return this;
  }

  public boolean isAutoCreateConsumers() {
    return autoCreateConsumers;
  }

  public RqueueNatsConfig setAutoCreateConsumers(boolean autoCreateConsumers) {
    this.autoCreateConsumers = autoCreateConsumers;
    return this;
  }

  public boolean isAutoCreateDlqStream() {
    return autoCreateDlqStream;
  }

  public RqueueNatsConfig setAutoCreateDlqStream(boolean autoCreateDlqStream) {
    this.autoCreateDlqStream = autoCreateDlqStream;
    return this;
  }

  public StreamDefaults getStreamDefaults() {
    return streamDefaults;
  }

  public RqueueNatsConfig setStreamDefaults(StreamDefaults streamDefaults) {
    this.streamDefaults = streamDefaults;
    return this;
  }

  public ConsumerDefaults getConsumerDefaults() {
    return consumerDefaults;
  }

  public RqueueNatsConfig setConsumerDefaults(ConsumerDefaults consumerDefaults) {
    this.consumerDefaults = consumerDefaults;
    return this;
  }

  public Duration getDefaultFetchWait() {
    return defaultFetchWait;
  }

  public RqueueNatsConfig setDefaultFetchWait(Duration defaultFetchWait) {
    this.defaultFetchWait = defaultFetchWait;
    return this;
  }

  // ---- nested defaults ----------------------------------------------------

  public static class StreamDefaults {
    private int replicas = 1;
    private StorageType storage = StorageType.File;
    private RetentionPolicy retention = RetentionPolicy.WorkQueue;
    private Duration duplicateWindow = Duration.ofMinutes(2);
    private long maxMsgs = -1;
    private long maxBytes = -1;

    public int getReplicas() {
      return replicas;
    }

    public StreamDefaults setReplicas(int replicas) {
      this.replicas = replicas;
      return this;
    }

    public StorageType getStorage() {
      return storage;
    }

    public StreamDefaults setStorage(StorageType storage) {
      this.storage = storage;
      return this;
    }

    public RetentionPolicy getRetention() {
      return retention;
    }

    public StreamDefaults setRetention(RetentionPolicy retention) {
      this.retention = retention;
      return this;
    }

    public Duration getDuplicateWindow() {
      return duplicateWindow;
    }

    public StreamDefaults setDuplicateWindow(Duration duplicateWindow) {
      this.duplicateWindow = duplicateWindow;
      return this;
    }

    public long getMaxMsgs() {
      return maxMsgs;
    }

    public StreamDefaults setMaxMsgs(long maxMsgs) {
      this.maxMsgs = maxMsgs;
      return this;
    }

    public long getMaxBytes() {
      return maxBytes;
    }

    public StreamDefaults setMaxBytes(long maxBytes) {
      this.maxBytes = maxBytes;
      return this;
    }
  }

  public static class ConsumerDefaults {
    private Duration ackWait = Duration.ofSeconds(30);
    private long maxDeliver = 5;
    private long maxAckPending = 1000;

    public Duration getAckWait() {
      return ackWait;
    }

    public ConsumerDefaults setAckWait(Duration ackWait) {
      this.ackWait = ackWait;
      return this;
    }

    public long getMaxDeliver() {
      return maxDeliver;
    }

    public ConsumerDefaults setMaxDeliver(long maxDeliver) {
      this.maxDeliver = maxDeliver;
      return this;
    }

    public long getMaxAckPending() {
      return maxAckPending;
    }

    public ConsumerDefaults setMaxAckPending(long maxAckPending) {
      this.maxAckPending = maxAckPending;
      return this;
    }
  }
}
