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
package com.github.sonus21.rqueue.listener;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.github.sonus21.TestBase;
import com.github.sonus21.rqueue.CoreUnitTest;
import com.github.sonus21.rqueue.annotation.RqueueListener;
import java.lang.reflect.Method;
import org.junit.jupiter.api.Test;

@CoreUnitTest
class ConsumerNameResolverTest extends TestBase {

  static class Sample {
    @RqueueListener(value = "q1")
    public void defaultName() {}

    @RqueueListener(value = "q1", consumerName = "explicit-consumer")
    public void overridden() {}
  }

  @Test
  void defaultsWhenAnnotationConsumerNameIsBlank() throws Exception {
    Method m = Sample.class.getMethod("defaultName");
    RqueueListener ann = m.getAnnotation(RqueueListener.class);
    assertEquals(
        "rqueue-q1-mybean#defaultName",
        ConsumerNameResolver.resolveConsumerName(ann, "mybean", "defaultName", "q1"));
  }

  @Test
  void usesExplicitNameWhenSet() throws Exception {
    Method m = Sample.class.getMethod("overridden");
    RqueueListener ann = m.getAnnotation(RqueueListener.class);
    assertEquals(
        "explicit-consumer",
        ConsumerNameResolver.resolveConsumerName(ann, "mybean", "overridden", "q1"));
  }

  @Test
  void nullAnnotationFallsBackToDefault() {
    assertEquals(
        "rqueue-qX-bean#m", ConsumerNameResolver.resolveConsumerName(null, "bean", "m", "qX"));
  }
}
