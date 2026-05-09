/*
 * Copyright (c) 2026 Sonu Kumar
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * You may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 */
package com.github.sonus21.rqueue.spring.boot;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.github.sonus21.rqueue.spring.boot.tests.SpringBootUnitTest;
import java.time.Duration;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for {@link RqueueNatsProperties}: default values, nested class accessors, and
 * property assignment.
 */
@SpringBootUnitTest
class RqueueNatsPropertiesTest {

  // ---- defaults ----

  @Test
  void defaultProperties_autoCreateStreamsTrue() {
    assertTrue(new RqueueNatsProperties().isAutoCreateStreams());
  }

  @Test
  void defaultProperties_autoCreateConsumersTrue() {
    assertTrue(new RqueueNatsProperties().isAutoCreateConsumers());
  }

  @Test
  void defaultProperties_autoCreateDlqStreamFalse() {
    assertFalse(new RqueueNatsProperties().isAutoCreateDlqStream());
  }

  @Test
  void defaultProperties_autoCreateKvBucketsTrue() {
    assertTrue(new RqueueNatsProperties().isAutoCreateKvBuckets());
  }

  @Test
  void defaultProperties_nestedObjectsNotNull() {
    RqueueNatsProperties props = new RqueueNatsProperties();
    assertNotNull(props.getConnection());
    assertNotNull(props.getStream());
    assertNotNull(props.getConsumer());
    assertNotNull(props.getNaming());
  }

  // ---- Connection nested class ----

  @Test
  void connectionDefaults_urlIsNull() {
    assertFalse(new RqueueNatsProperties.Connection().getUrl() != null
        && !new RqueueNatsProperties.Connection().getUrl().isEmpty());
  }

  @Test
  void connectionDefaults_maxReconnectsNegativeOne() {
    assertEquals(-1, new RqueueNatsProperties.Connection().getMaxReconnects());
  }

  @Test
  void connectionDefaults_tlsFalse() {
    assertFalse(new RqueueNatsProperties.Connection().isTls());
  }

  @Test
  void connection_settersAndGetters() {
    RqueueNatsProperties.Connection c = new RqueueNatsProperties.Connection();
    c.setUrl("nats://localhost:4222");
    c.setUsername("user");
    c.setPassword("pass");
    c.setToken("token123");
    c.setConnectionName("test-conn");
    c.setMaxReconnects(5);
    c.setTls(true);
    c.setConnectTimeout(Duration.ofSeconds(5));
    c.setReconnectWait(Duration.ofSeconds(2));
    c.setPingInterval(Duration.ofSeconds(30));

    assertEquals("nats://localhost:4222", c.getUrl());
    assertEquals("user", c.getUsername());
    assertEquals("pass", c.getPassword());
    assertEquals("token123", c.getToken());
    assertEquals("test-conn", c.getConnectionName());
    assertEquals(5, c.getMaxReconnects());
    assertTrue(c.isTls());
    assertEquals(Duration.ofSeconds(5), c.getConnectTimeout());
    assertEquals(Duration.ofSeconds(2), c.getReconnectWait());
    assertEquals(Duration.ofSeconds(30), c.getPingInterval());
  }

  // ---- Stream nested class ----

  @Test
  void streamDefaults_replicas1() {
    assertEquals(1, new RqueueNatsProperties.Stream().getReplicas());
  }

  @Test
  void streamDefaults_storageFILE() {
    assertEquals("FILE", new RqueueNatsProperties.Stream().getStorage());
  }

  @Test
  void streamDefaults_retentionLIMITS() {
    assertEquals("LIMITS", new RqueueNatsProperties.Stream().getRetention());
  }

  @Test
  void streamDefaults_maxAge14Days() {
    assertEquals(Duration.ofDays(14), new RqueueNatsProperties.Stream().getMaxAge());
  }

  @Test
  void streamDefaults_maxBytesNegativeOne() {
    assertEquals(-1L, new RqueueNatsProperties.Stream().getMaxBytes());
  }

  @Test
  void streamDefaults_maxMessagesNegativeOne() {
    assertEquals(-1L, new RqueueNatsProperties.Stream().getMaxMessages());
  }

  @Test
  void stream_settersAndGetters() {
    RqueueNatsProperties.Stream s = new RqueueNatsProperties.Stream();
    s.setReplicas(3);
    s.setStorage("MEMORY");
    s.setRetention("WORKQUEUE");
    s.setMaxAge(Duration.ofDays(7));
    s.setMaxBytes(1024L);
    s.setMaxMessages(5000L);
    s.setDiscardPolicy("NEW");

    assertEquals(3, s.getReplicas());
    assertEquals("MEMORY", s.getStorage());
    assertEquals("WORKQUEUE", s.getRetention());
    assertEquals(Duration.ofDays(7), s.getMaxAge());
    assertEquals(1024L, s.getMaxBytes());
    assertEquals(5000L, s.getMaxMessages());
    assertEquals("NEW", s.getDiscardPolicy());
  }

  // ---- Consumer nested class ----

  @Test
  void consumerDefaults_ackWait30Seconds() {
    assertEquals(Duration.ofSeconds(30), new RqueueNatsProperties.Consumer().getAckWait());
  }

  @Test
  void consumerDefaults_maxDeliver3() {
    assertEquals(3L, new RqueueNatsProperties.Consumer().getMaxDeliver());
  }

  @Test
  void consumerDefaults_maxAckPending1000() {
    assertEquals(1000L, new RqueueNatsProperties.Consumer().getMaxAckPending());
  }

  @Test
  void consumerDefaults_fetchWait2Seconds() {
    assertEquals(Duration.ofSeconds(2), new RqueueNatsProperties.Consumer().getFetchWait());
  }

  @Test
  void consumer_settersAndGetters() {
    RqueueNatsProperties.Consumer c = new RqueueNatsProperties.Consumer();
    c.setAckWait(Duration.ofSeconds(60));
    c.setMaxDeliver(10L);
    c.setMaxAckPending(500L);
    c.setFetchWait(Duration.ofSeconds(5));

    assertEquals(Duration.ofSeconds(60), c.getAckWait());
    assertEquals(10L, c.getMaxDeliver());
    assertEquals(500L, c.getMaxAckPending());
    assertEquals(Duration.ofSeconds(5), c.getFetchWait());
  }

  // ---- Naming nested class ----

  @Test
  void namingDefaults_streamPrefixRqueueJs() {
    assertEquals("rqueue-js-", new RqueueNatsProperties.Naming().getStreamPrefix());
  }

  @Test
  void namingDefaults_subjectPrefixRqueueJs() {
    assertEquals("rqueue.js.", new RqueueNatsProperties.Naming().getSubjectPrefix());
  }

  @Test
  void namingDefaults_dlqSuffixDlq() {
    assertEquals("-dlq", new RqueueNatsProperties.Naming().getDlqSuffix());
  }

  @Test
  void naming_settersAndGetters() {
    RqueueNatsProperties.Naming n = new RqueueNatsProperties.Naming();
    n.setStreamPrefix("myapp-");
    n.setSubjectPrefix("myapp.");
    n.setDlqSuffix("-dead");

    assertEquals("myapp-", n.getStreamPrefix());
    assertEquals("myapp.", n.getSubjectPrefix());
    assertEquals("-dead", n.getDlqSuffix());
  }

  // ---- top-level setters ----

  @Test
  void setAutoCreateStreams_false_storesValue() {
    RqueueNatsProperties props = new RqueueNatsProperties();
    props.setAutoCreateStreams(false);
    assertFalse(props.isAutoCreateStreams());
  }

  @Test
  void setAutoCreateDlqStream_true_storesValue() {
    RqueueNatsProperties props = new RqueueNatsProperties();
    props.setAutoCreateDlqStream(true);
    assertTrue(props.isAutoCreateDlqStream());
  }
}
