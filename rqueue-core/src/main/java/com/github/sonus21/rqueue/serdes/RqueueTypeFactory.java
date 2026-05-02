package com.github.sonus21.rqueue.serdes;

public interface RqueueTypeFactory {

  TypeEnvelop create(Class<?> clazz);

  TypeEnvelop create(Class<?> parametrized, Class<?>... parameterClasses);
}
