package com.github.sonus21.rqueue.utils.condition;

import org.springframework.context.annotation.Condition;
import org.springframework.context.annotation.ConditionContext;
import org.springframework.core.type.AnnotatedTypeMetadata;

public class RqueueEnabled implements Condition {

  @Override
  public boolean matches(ConditionContext context, AnnotatedTypeMetadata metadata) {
    Boolean enabled = context.getEnvironment().getProperty("rqueue.enabled", Boolean.class);
    return enabled == null || Boolean.TRUE.equals(enabled);
  }
}
