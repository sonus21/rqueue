/*
 * Copyright (c) 2024-2026 Sonu Kumar
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * You may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 */
package com.github.sonus21.rqueue.serdes;

import tools.jackson.databind.JavaType;
import java.io.IOException;

/**
 * Serialization / deserialization contract used by the NATS backend.
 */
public interface RqueueSerDes {

  <T> byte[] serialize(T value) throws IOException;

  <T> String serializeAsString(T value) throws IOException;

  <T> T deserialize(byte[] bytes, Class<T> klass) throws IOException;

  <T> T deserialize(byte[] msg, TypeEnvelop type) throws IOException;

  <T> T deserialize(String msg, Class<T> klass) throws IOException;
}
