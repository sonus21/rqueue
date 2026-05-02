/*
 * Copyright (c) 2026 Sonu Kumar
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * You may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 */
package com.github.sonus21.rqueue.serdes;

import java.io.IOException;
import tools.jackson.databind.ObjectMapper;

/**
 * {@link RqueueSerDes} implementation backed by a Jackson {@link ObjectMapper}.
 */
public class RqJacksonSerDes implements RqueueSerDes {

  private final ObjectMapper mapper;

  public RqJacksonSerDes(ObjectMapper mapper) {
    this.mapper = mapper;
  }

  @Override
  public <T> String serializeAsString(T value) throws IOException {
    return mapper.writeValueAsString(value);
  }

  @Override
  public <T> byte[] serialize(T value) throws IOException {
    return mapper.writeValueAsBytes(value);
  }

  @Override
  public <T> T deserialize(byte[] bytes, Class<T> klass) throws IOException {
    return mapper.readValue(bytes, klass);
  }

  @Override
  public <T> T deserialize(byte[] msg, TypeEnvelop type) {
    return mapper.readValue(msg, ((JacksonTypeEnvelop) type).getJavaType());
  }

  @Override
  public <T> T deserialize(String msg, Class<T> klass) throws IOException {
    return mapper.readValue(msg, klass);
  }
}
