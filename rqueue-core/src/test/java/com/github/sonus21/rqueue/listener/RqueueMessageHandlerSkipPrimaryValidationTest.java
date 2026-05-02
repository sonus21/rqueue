/*
 * Copyright (c) 2026 Sonu Kumar
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
package com.github.sonus21.rqueue.listener;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.github.sonus21.TestBase;
import com.github.sonus21.rqueue.CoreUnitTest;
import com.github.sonus21.rqueue.annotation.RqueueHandler;
import com.github.sonus21.rqueue.annotation.RqueueListener;
import com.github.sonus21.rqueue.core.DefaultRqueueMessageConverter;
import org.junit.jupiter.api.Test;
import org.springframework.context.support.StaticApplicationContext;

@CoreUnitTest
class RqueueMessageHandlerSkipPrimaryValidationTest extends TestBase {

  @RqueueListener(value = "skip-validation-q")
  static class HandlerWithoutPrimary {
    @RqueueHandler
    public void m1(String s) {}

    @RqueueHandler
    public void m2(String s) {}
  }

  /** Subclass that flips the capability flag before super.afterPropertiesSet(). */
  static class NatsLikeHandler extends RqueueMessageHandler {
    NatsLikeHandler() {
      super(new DefaultRqueueMessageConverter(), true);
      setPrimaryHandlerDispatchEnabled(false);
    }
  }

  @Test
  void skipsPrimaryValidationWhenCapabilityDisabled() {
    StaticApplicationContext ctx = new StaticApplicationContext();
    ctx.registerSingleton("h", HandlerWithoutPrimary.class);
    ctx.registerSingleton("rqueueMessageHandler", NatsLikeHandler.class);
    // Without the flag this would throw because there's no primary among multiple @RqueueHandler.
    assertDoesNotThrow(ctx::refresh);
    NatsLikeHandler handler = ctx.getBean("rqueueMessageHandler", NatsLikeHandler.class);
    assertFalse(handler.isPrimaryHandlerDispatchEnabled());
  }

  @Test
  void defaultEnabledFlagIsTrue() {
    RqueueMessageHandler h = new RqueueMessageHandler(new DefaultRqueueMessageConverter());
    assertTrue(h.isPrimaryHandlerDispatchEnabled());
  }
}
