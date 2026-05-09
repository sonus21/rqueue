/*
 * Copyright (c) 2026 Sonu Kumar
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * You may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 */
package com.github.sonus21.rqueue.spring.tests.unit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.when;

import com.github.sonus21.rqueue.config.Backend;
import com.github.sonus21.rqueue.spring.EnableRqueue;
import com.github.sonus21.rqueue.spring.RqueueBackendImportSelector;
import com.github.sonus21.rqueue.spring.RqueueListenerConfig;
import com.github.sonus21.rqueue.spring.RqueueNatsListenerConfig;
import com.github.sonus21.rqueue.spring.tests.SpringUnitTest;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.springframework.core.type.AnnotationMetadata;

/**
 * Unit tests for {@link RqueueBackendImportSelector}: verifies correct configuration classes are
 * imported for each backend value.
 */
@SpringUnitTest
class RqueueBackendImportSelectorTest {

  @Mock
  private AnnotationMetadata metadata;

  private final RqueueBackendImportSelector selector = new RqueueBackendImportSelector();

  private void stubBackend(Backend backend) {
    Map<String, Object> attrs = new HashMap<>();
    attrs.put("backend", backend);
    when(metadata.getAnnotationAttributes(EnableRqueue.class.getName())).thenReturn(attrs);
  }

  @Test
  void selectImports_noAnnotationAttributes_returnsOnlyRedisConfig() {
    when(metadata.getAnnotationAttributes(EnableRqueue.class.getName())).thenReturn(null);
    String[] imports = selector.selectImports(metadata);
    assertEquals(1, imports.length);
    assertEquals(RqueueListenerConfig.class.getName(), imports[0]);
  }

  @Test
  void selectImports_backendRedis_returnsOnlyListenerConfig() {
    stubBackend(Backend.REDIS);
    String[] imports = selector.selectImports(metadata);
    assertEquals(1, imports.length);
    assertEquals(RqueueListenerConfig.class.getName(), imports[0]);
  }

  @Test
  void selectImports_backendNats_returnsBothConfigs() {
    stubBackend(Backend.NATS);
    String[] imports = selector.selectImports(metadata);
    assertEquals(2, imports.length);
    assertTrue(Arrays.asList(imports).contains(RqueueListenerConfig.class.getName()));
    assertTrue(Arrays.asList(imports).contains(RqueueNatsListenerConfig.class.getName()));
  }

  @Test
  void selectImports_backendAsString_nats_returnsBothConfigs() {
    Map<String, Object> attrs = new HashMap<>();
    attrs.put("backend", "NATS");
    when(metadata.getAnnotationAttributes(EnableRqueue.class.getName())).thenReturn(attrs);
    String[] imports = selector.selectImports(metadata);
    assertEquals(2, imports.length);
  }

  @Test
  void selectImports_backendAsString_redis_returnsOnlyListenerConfig() {
    Map<String, Object> attrs = new HashMap<>();
    attrs.put("backend", "REDIS");
    when(metadata.getAnnotationAttributes(EnableRqueue.class.getName())).thenReturn(attrs);
    String[] imports = selector.selectImports(metadata);
    assertEquals(1, imports.length);
    assertEquals(RqueueListenerConfig.class.getName(), imports[0]);
  }

  @Test
  void selectImports_backendAsUnknownString_defaultsToRedis() {
    Map<String, Object> attrs = new HashMap<>();
    attrs.put("backend", "UNKNOWN_BACKEND");
    when(metadata.getAnnotationAttributes(EnableRqueue.class.getName())).thenReturn(attrs);
    String[] imports = selector.selectImports(metadata);
    assertEquals(1, imports.length);
    assertEquals(RqueueListenerConfig.class.getName(), imports[0]);
  }

  @Test
  void selectImports_backendAttributeNull_defaultsToRedis() {
    Map<String, Object> attrs = new HashMap<>();
    attrs.put("backend", null);
    when(metadata.getAnnotationAttributes(EnableRqueue.class.getName())).thenReturn(attrs);
    String[] imports = selector.selectImports(metadata);
    assertEquals(1, imports.length);
    assertEquals(RqueueListenerConfig.class.getName(), imports[0]);
  }
}
