/*
 * Copyright (c) 2020-2026 Sonu Kumar
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

package com.github.sonus21.rqueue.core.spi;

import java.util.Map;
import java.util.ServiceLoader;

public final class MessageBrokerLoader {

  private MessageBrokerLoader() {
  }

  public static MessageBroker load(String name, Map<String, String> config) {
    for (MessageBrokerFactory f : ServiceLoader.load(MessageBrokerFactory.class)) {
      if (f.name().equalsIgnoreCase(name)) {
        return f.create(config);
      }
    }
    throw new IllegalArgumentException("No MessageBrokerFactory found for backend: " + name);
  }
}
