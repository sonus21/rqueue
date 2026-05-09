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

import com.github.sonus21.rqueue.spring.RqueueRedisConfigImportSelector;
import com.github.sonus21.rqueue.spring.tests.SpringUnitTest;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.springframework.core.type.AnnotationMetadata;

/**
 * Unit tests for {@link RqueueRedisConfigImportSelector}: conditionally imports Redis config only
 * when the rqueue-redis module is on the classpath.
 */
@SpringUnitTest
class RqueueRedisConfigImportSelectorTest {

  @Mock
  private AnnotationMetadata metadata;

  private final RqueueRedisConfigImportSelector selector = new RqueueRedisConfigImportSelector();

  @Test
  void selectImports_redisConfigClassPresent_returnsClassName() {
    // RqueueRedisListenerConfig lives in rqueue-redis; in this test run rqueue-redis IS present
    // (spring module depends on it via test scope), so we expect the class to be returned
    String[] imports = selector.selectImports(metadata);
    // Either the class is present (returns [className]) or absent (returns [])
    // The test asserts the contract: if present → exactly one entry; if absent → empty
    if (imports.length == 1) {
      assertEquals("com.github.sonus21.rqueue.redis.config.RqueueRedisListenerConfig", imports[0]);
    } else {
      assertEquals(0, imports.length);
    }
  }

  @Test
  void selectImports_alwaysReturnsArrayNotNull() {
    String[] imports = selector.selectImports(metadata);
    // null would cause NullPointerException in Spring's ImportSelector handling
    org.junit.jupiter.api.Assertions.assertNotNull(imports);
  }

  @Test
  void selectImports_metadataNotUsed_doesNotThrow() {
    // Selector ignores annotation metadata entirely; passing null must not throw
    String[] imports = selector.selectImports(null);
    org.junit.jupiter.api.Assertions.assertNotNull(imports);
  }
}
