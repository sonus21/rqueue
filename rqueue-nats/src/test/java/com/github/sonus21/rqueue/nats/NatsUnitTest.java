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

import com.github.sonus21.junit.TestTracerExtension;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.extension.ExtendWith;

/**
 * NATS unit tests; tagged so CI's {@code unit_test} job picks them up. Intentionally does not
 * include {@code MockitoExtension} — the existing tests use {@code Mockito.mock()} directly and
 * adding the extension would activate strict-stubbing and flag setup-only stubs as unnecessary.
 */
@Target({ElementType.TYPE})
@Retention(RetentionPolicy.RUNTIME)
@Tag("unit")
@Tag("nats")
@ExtendWith(TestTracerExtension.class)
public @interface NatsUnitTest {}
