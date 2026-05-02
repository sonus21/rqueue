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
package com.github.sonus21.rqueue.spring;

import com.github.sonus21.rqueue.config.Backend;
import org.springframework.context.annotation.ImportSelector;
import org.springframework.core.type.AnnotationMetadata;

/**
 * Selects the listener configuration classes to import based on {@link EnableRqueue#backend()}.
 *
 * <ul>
 *   <li>{@link Backend#REDIS} (default) — only {@code RqueueListenerConfig}.</li>
 *   <li>{@link Backend#NATS} — both {@code RqueueListenerConfig} and {@code
 *       RqueueNatsListenerConfig}.</li>
 * </ul>
 */
public class RqueueBackendImportSelector implements ImportSelector {
  @Override
  public String[] selectImports(AnnotationMetadata importingClassMetadata) {
    Backend backend = Backend.REDIS;
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
        // keep default
      }
    }
    if (backend == Backend.NATS) {
      return new String[] {
        RqueueListenerConfig.class.getName(), RqueueNatsListenerConfig.class.getName()
      };
    }
    return new String[] {RqueueListenerConfig.class.getName()};
  }
}
