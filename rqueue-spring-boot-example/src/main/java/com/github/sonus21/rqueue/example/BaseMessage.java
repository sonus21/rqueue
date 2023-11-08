package com.github.sonus21.rqueue.example;

import java.util.LinkedHashMap;
import java.util.Map;
import lombok.Data;
import org.springframework.util.CollectionUtils;

@Data
public class BaseMessage {

  private Map<String, Object> metadata;

  public void addMetadata(String name, Object value) {
    if (CollectionUtils.isEmpty(metadata)) {
      metadata = new LinkedHashMap<>();
    }
    metadata.put(name, value);
  }
}
