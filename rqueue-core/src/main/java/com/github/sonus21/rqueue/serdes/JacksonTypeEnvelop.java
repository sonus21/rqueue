package com.github.sonus21.rqueue.serdes;

import lombok.AllArgsConstructor;
import lombok.Getter;
import tools.jackson.databind.JavaType;

@AllArgsConstructor
@Getter
public class JacksonTypeEnvelop implements TypeEnvelop {

  private final JavaType javaType;
}
