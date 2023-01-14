/*
 * Copyright (c) 2019-2023 Sonu Kumar
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

package com.github.sonus21.rqueue.config;

import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.Tags;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Map.Entry;
import lombok.Getter;
import lombok.Setter;

/**
 * RqueueMetrics provides all possible configurations available in Rqueue library for metrics.
 *
 * <p>Rqueue can be configured to count the failure execution, total execution, and can have set of
 * tags.
 */
@Getter
@Setter
public abstract class MetricsProperties {

  /**
   * List of tags to be used while publishing metrics.
   */
  private Map<String, String> tags = new LinkedHashMap<>();

  /**
   * what type of counting feature is enabled, by default counting feature is disabled.
   */
  private Count count = new Count();

  private Tags metricTags = Tags.empty();

  /**
   * Get Tags object that can be used in metric. Tags can be either configured manually or using
   * properties or XML file.
   *
   * @return Tags object
   */
  public Tags getMetricTags() {
    Tag tag = metricTags.stream().findFirst().orElse(null);
    if (tag == null && !tags.isEmpty()) {
      for (Entry<String, String> entry : tags.entrySet()) {
        metricTags = metricTags.and(entry.getKey(), entry.getValue());
      }
    }
    return metricTags;
  }

  public void setMetricTags(Tags tags) {
    metricTags = tags;
  }

  public boolean countExecution() {
    return count.isExecution();
  }

  public boolean countFailure() {
    return count.isFailure();
  }

  @Getter
  @Setter
  public static class Count {

    /**
     * Count all execution success and failure
     *
     * <p>each method invocation is counted once, which means if a message is retried N times then
     * it will be counted as N
     */
    private boolean execution = false;
    /**
     * count failure execution, count increases whenever invocation failure, or it fails due to
     * deserialization etc. Any types of failure would be counted.
     */
    private boolean failure = false;
  }
}
