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

import com.github.sonus21.junit.TestTracerExtension;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.extension.ExtendWith;

/**
 * Meta-annotation for Docker-gated JetStream integration tests. Carries only the {@code nats}
 * tag so the existing Redis-driven {@code integration_test} and {@code reactive_integration_test}
 * jobs don't try to run them; they're picked up exclusively by the dedicated
 * {@code nats_integration_test} CI job via {@code -DincludeTags=nats}.
 */
@Target({ElementType.TYPE})
@Retention(RetentionPolicy.RUNTIME)
@Tag("nats")
@ExtendWith(TestTracerExtension.class)
public @interface NatsIntegrationTest {}
