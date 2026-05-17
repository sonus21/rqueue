/*
 * Copyright (c) 2026 Sonu Kumar
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * You may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 */
package com.github.sonus21.rqueue.nats.kv;

/** Utility methods for converting rqueue keys into NATS JetStream KV keys. */
public final class NatsKvKeys {

  private NatsKvKeys() {}

  /**
   * NATS KV keys do not need Redis Cluster hash tags. Strip any {@code {name}} tag first, then
   * coerce the remaining key into the conservative KV key character set used by this module.
   */
  public static String sanitize(String key) {
    if (key == null) {
      return "_";
    }
    String sanitized = key.replaceAll("\\{([^{}]*)}", "$1").replaceAll("[^A-Za-z0-9_=.-]", "_");
    return sanitized.isEmpty() ? "_" : sanitized;
  }
}
