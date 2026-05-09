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

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.when;

import com.github.sonus21.rqueue.spring.MetricsEnabled;
import com.github.sonus21.rqueue.spring.tests.SpringUnitTest;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.springframework.context.annotation.ConditionContext;
import org.springframework.core.type.AnnotatedTypeMetadata;

/**
 * Unit tests for {@link MetricsEnabled}: presence/absence of MeterRegistry on the classpath.
 */
@SpringUnitTest
class MetricsEnabledTest {

  @Mock
  private ConditionContext context;

  @Mock
  private AnnotatedTypeMetadata metadata;

  private final MetricsEnabled condition = new MetricsEnabled();

  @BeforeEach
  void setUp() {
    when(context.getClassLoader()).thenReturn(getClass().getClassLoader());
  }

  @Test
  void matches_meterRegistryOnClasspath_returnsTrue() {
    // micrometer-core is on the test classpath so MeterRegistry should be present
    boolean result = condition.matches(context, metadata);
    assertTrue(
        result, "Expected MeterRegistry to be on the test classpath (micrometer-core dependency)");
  }

  @Test
  void matches_classLoaderWithoutMicrometer_returnsFalse() {
    ClassLoader empty = new ClassLoader(null) {
      @Override
      public Class<?> loadClass(String name) throws ClassNotFoundException {
        throw new ClassNotFoundException(name);
      }
    };
    when(context.getClassLoader()).thenReturn(empty);
    assertFalse(condition.matches(context, metadata));
  }
}
