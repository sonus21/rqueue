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

import com.github.sonus21.rqueue.spring.NatsBackendCondition;
import com.github.sonus21.rqueue.spring.tests.SpringUnitTest;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.springframework.context.annotation.ConditionContext;
import org.springframework.core.env.Environment;
import org.springframework.core.type.AnnotatedTypeMetadata;

/**
 * Unit tests for {@link NatsBackendCondition}: class-present + backend-property combinations.
 */
@SpringUnitTest
class NatsBackendConditionTest {

  @Mock
  private ConditionContext context;

  @Mock
  private Environment environment;

  @Mock
  private AnnotatedTypeMetadata metadata;

  private final NatsBackendCondition condition = new NatsBackendCondition();

  @BeforeEach
  void setUp() {
    // Real classloader so ClassUtils.isPresent works against the actual test classpath
    when(context.getClassLoader()).thenReturn(getClass().getClassLoader());
    when(context.getEnvironment()).thenReturn(environment);
  }

  @Test
  void matches_natsOnClasspathAndBackendNats_returnsTrue() {
    when(environment.getProperty("rqueue.backend")).thenReturn("nats");
    // io.nats.client.JetStream is on the test classpath (rqueue-nats dependency)
    assertTrue(condition.matches(context, metadata));
  }

  @Test
  void matches_natsOnClasspathAndBackendNatsUpperCase_returnsTrueIgnoringCase() {
    when(environment.getProperty("rqueue.backend")).thenReturn("NATS");
    assertTrue(condition.matches(context, metadata));
  }

  @Test
  void matches_natsOnClasspathAndBackendRedis_returnsFalse() {
    when(environment.getProperty("rqueue.backend")).thenReturn("redis");
    assertFalse(condition.matches(context, metadata));
  }

  @Test
  void matches_natsOnClasspathAndBackendNull_returnsFalse() {
    when(environment.getProperty("rqueue.backend")).thenReturn(null);
    assertFalse(condition.matches(context, metadata));
  }

  @Test
  void matches_natsOnClasspathAndBackendEmpty_returnsFalse() {
    when(environment.getProperty("rqueue.backend")).thenReturn("");
    assertFalse(condition.matches(context, metadata));
  }

  @Test
  void matches_classLoaderCannotFindJetStream_returnsFalse() {
    // Use a classloader that cannot find io.nats.client.JetStream
    ClassLoader empty = new ClassLoader(null) {
      @Override
      public Class<?> loadClass(String name) throws ClassNotFoundException {
        throw new ClassNotFoundException(name);
      }
    };
    when(context.getClassLoader()).thenReturn(empty);
    when(environment.getProperty("rqueue.backend")).thenReturn("nats");
    assertFalse(condition.matches(context, metadata));
  }

  @Test
  void matches_natsOnClasspathAndBackendOtherValue_returnsFalse() {
    when(environment.getProperty("rqueue.backend")).thenReturn("kafka");
    assertFalse(condition.matches(context, metadata));
  }
}
