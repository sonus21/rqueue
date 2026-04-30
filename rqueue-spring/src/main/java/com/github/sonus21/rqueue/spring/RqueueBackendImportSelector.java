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
package com.github.sonus21.rqueue.spring;

import java.util.ArrayList;
import java.util.List;
import org.springframework.context.annotation.ImportSelector;
import org.springframework.core.type.AnnotationMetadata;

/**
 * Selects the listener configuration classes to import based on {@link EnableRqueue#backend()}.
 *
 * <ul>
 *   <li>{@link Backend#REDIS} — only the legacy {@code RqueueListenerConfig}</li>
 *   <li>{@link Backend#NATS} — base config plus {@code RqueueNatsListenerConfig} unconditionally
 *   <li>{@link Backend#AUTO} — base config; the NATS config is gated by
 *       {@link NatsBackendCondition} so it activates only when jnats is on the classpath
 *       <em>and</em> {@code rqueue.backend=nats} is set.
 * </ul>
 */
public class RqueueBackendImportSelector implements ImportSelector {
  @Override
  public String[] selectImports(AnnotationMetadata importingClassMetadata) {
    Backend backend = Backend.AUTO;
    Object raw =
        importingClassMetadata.getAnnotationAttributes(EnableRqueue.class.getName()) == null
            ? null
            : importingClassMetadata
                .getAnnotationAttributes(EnableRqueue.class.getName())
                .get("backend");
    if (raw instanceof Backend b) {
      backend = b;
    } else if (raw != null) {
      try {
        backend = Backend.valueOf(raw.toString());
      } catch (IllegalArgumentException ignored) {
        // keep AUTO
      }
    }
    List<String> imports = new ArrayList<>();
    imports.add(RqueueListenerConfig.class.getName());
    if (backend == Backend.NATS) {
      imports.add(RqueueNatsListenerConfig.class.getName());
    } else if (backend == Backend.AUTO) {
      // Conditionally registered via @Conditional in a thin wrapper.
      imports.add(ConditionalNatsConfig.class.getName());
    }
    return imports.toArray(new String[0]);
  }
}
