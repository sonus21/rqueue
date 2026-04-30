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
import java.util.List;
import org.springframework.boot.context.properties.ConfigurationProperties;

/** Configuration properties for the optional NATS / JetStream backend. */
@ConfigurationProperties(prefix = "rqueue.nats")
public class RqueueNatsProperties {

  private Connection connection = new Connection();
  private Stream stream = new Stream();
  private Consumer consumer = new Consumer();
  private Naming naming = new Naming();
  private boolean autoCreateStreams = true;
  private boolean autoCreateConsumers = true;
  private boolean autoCreateDlqStream = true;

  public Connection getConnection() {
    return connection;
  }

  public void setConnection(Connection connection) {
    this.connection = connection;
  }

  public Stream getStream() {
    return stream;
  }

  public void setStream(Stream stream) {
    this.stream = stream;
  }

  public Consumer getConsumer() {
    return consumer;
  }

  public void setConsumer(Consumer consumer) {
    this.consumer = consumer;
  }

  public Naming getNaming() {
    return naming;
  }

  public void setNaming(Naming naming) {
    this.naming = naming;
  }

  public boolean isAutoCreateStreams() {
    return autoCreateStreams;
  }

  public void setAutoCreateStreams(boolean autoCreateStreams) {
    this.autoCreateStreams = autoCreateStreams;
  }

  public boolean isAutoCreateConsumers() {
    return autoCreateConsumers;
  }

  public void setAutoCreateConsumers(boolean autoCreateConsumers) {
    this.autoCreateConsumers = autoCreateConsumers;
  }

  public boolean isAutoCreateDlqStream() {
    return autoCreateDlqStream;
  }

  public void setAutoCreateDlqStream(boolean autoCreateDlqStream) {
    this.autoCreateDlqStream = autoCreateDlqStream;
  }

  public static class Connection {
    private String url;
    private List<String> urls;
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

    public String getUrl() {
      return url;
    }

    public void setUrl(String url) {
      this.url = url;
    }

    public List<String> getUrls() {
      return urls;
    }

    public void setUrls(List<String> urls) {
      this.urls = urls;
    }

    public String getCredentialsPath() {
      return credentialsPath;
    }

    public void setCredentialsPath(String credentialsPath) {
      this.credentialsPath = credentialsPath;
    }

    public String getUsername() {
      return username;
    }

    public void setUsername(String username) {
      this.username = username;
    }

    public String getPassword() {
      return password;
    }

    public void setPassword(String password) {
      this.password = password;
    }

    public String getToken() {
      return token;
    }

    public void setToken(String token) {
      this.token = token;
    }

    public boolean isTls() {
      return tls;
    }

    public void setTls(boolean tls) {
      this.tls = tls;
    }

    public String getConnectionName() {
      return connectionName;
    }

    public void setConnectionName(String connectionName) {
      this.connectionName = connectionName;
    }

    public Duration getConnectTimeout() {
      return connectTimeout;
    }

    public void setConnectTimeout(Duration connectTimeout) {
      this.connectTimeout = connectTimeout;
    }

    public Duration getReconnectWait() {
      return reconnectWait;
    }

    public void setReconnectWait(Duration reconnectWait) {
      this.reconnectWait = reconnectWait;
    }

    public int getMaxReconnects() {
      return maxReconnects;
    }

    public void setMaxReconnects(int maxReconnects) {
      this.maxReconnects = maxReconnects;
    }

    public Duration getPingInterval() {
      return pingInterval;
    }

    public void setPingInterval(Duration pingInterval) {
      this.pingInterval = pingInterval;
    }
  }

  public static class Stream {
    private int replicas = 1;
    private String storage = "FILE";
    private Duration maxAge;
    private long maxBytes = -1;
    private long maxMessages = -1;
    private String discardPolicy = "OLD";
    private Duration duplicateWindow = Duration.ofMinutes(2);

    public int getReplicas() {
      return replicas;
    }

    public void setReplicas(int replicas) {
      this.replicas = replicas;
    }

    public String getStorage() {
      return storage;
    }

    public void setStorage(String storage) {
      this.storage = storage;
    }

    public Duration getMaxAge() {
      return maxAge;
    }

    public void setMaxAge(Duration maxAge) {
      this.maxAge = maxAge;
    }

    public long getMaxBytes() {
      return maxBytes;
    }

    public void setMaxBytes(long maxBytes) {
      this.maxBytes = maxBytes;
    }

    public long getMaxMessages() {
      return maxMessages;
    }

    public void setMaxMessages(long maxMessages) {
      this.maxMessages = maxMessages;
    }

    public String getDiscardPolicy() {
      return discardPolicy;
    }

    public void setDiscardPolicy(String discardPolicy) {
      this.discardPolicy = discardPolicy;
    }

    public Duration getDuplicateWindow() {
      return duplicateWindow;
    }

    public void setDuplicateWindow(Duration duplicateWindow) {
      this.duplicateWindow = duplicateWindow;
    }
  }

  public static class Consumer {
    private Duration ackWait = Duration.ofSeconds(30);
    private long maxDeliver = 5;
    private long maxAckPending = 1000;
    private int fetchBatch = 1;
    private Duration fetchWait = Duration.ofSeconds(2);

    public Duration getAckWait() {
      return ackWait;
    }

    public void setAckWait(Duration ackWait) {
      this.ackWait = ackWait;
    }

    public long getMaxDeliver() {
      return maxDeliver;
    }

    public void setMaxDeliver(long maxDeliver) {
      this.maxDeliver = maxDeliver;
    }

    public long getMaxAckPending() {
      return maxAckPending;
    }

    public void setMaxAckPending(long maxAckPending) {
      this.maxAckPending = maxAckPending;
    }

    public int getFetchBatch() {
      return fetchBatch;
    }

    public void setFetchBatch(int fetchBatch) {
      this.fetchBatch = fetchBatch;
    }

    public Duration getFetchWait() {
      return fetchWait;
    }

    public void setFetchWait(Duration fetchWait) {
      this.fetchWait = fetchWait;
    }
  }

  public static class Naming {
    private String streamPrefix = "rqueue-";
    private String subjectPrefix = "rqueue.";
    private String dlqSuffix = "-dlq";

    public String getStreamPrefix() {
      return streamPrefix;
    }

    public void setStreamPrefix(String streamPrefix) {
      this.streamPrefix = streamPrefix;
    }

    public String getSubjectPrefix() {
      return subjectPrefix;
    }

    public void setSubjectPrefix(String subjectPrefix) {
      this.subjectPrefix = subjectPrefix;
    }

    public String getDlqSuffix() {
      return dlqSuffix;
    }

    public void setDlqSuffix(String dlqSuffix) {
      this.dlqSuffix = dlqSuffix;
    }
  }
}
