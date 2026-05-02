package com.github.sonus21.rqueue.serdes;

import tools.jackson.databind.JavaType;
import tools.jackson.databind.ObjectMapper;

public class RqJacksonTypeFactory implements RqueueTypeFactory {

  private final ObjectMapper mapper;

  public RqJacksonTypeFactory(ObjectMapper mapper) {
    this.mapper = mapper;
  }

  @Override
  public TypeEnvelop create(Class<?> clazz) {
    JavaType type = mapper.getTypeFactory().constructType(clazz);
    return new JacksonTypeEnvelop(type);
  }

  @Override
  public TypeEnvelop create(Class<?> parametrized, Class<?>... parameterClasses) {
    JavaType type = mapper.getTypeFactory().constructParametricType(parametrized, parameterClasses);
    return new JacksonTypeEnvelop(type);
  }
}
