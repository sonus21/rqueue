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

package com.github.sonus21.rqueue.metrics;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.github.sonus21.TestBase;
import com.github.sonus21.rqueue.CoreUnitTest;
import com.github.sonus21.rqueue.config.MetricsProperties;
import io.micrometer.core.instrument.Tags;
import java.util.Collections;
import java.util.Map;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.junit.jupiter.api.Test;

@CoreUnitTest
class MetricsPropertiesTest extends TestBase {

  private final MetricProperties metricProperties = new MetricProperties();

  @Test
  void getTags() {
    assertEquals(0, metricProperties.getTags().size());
  }

  @Test
  void setMetricTags() {
    metricProperties.setMetricTags(Tags.of("test", "test"));
    assertEquals(Tags.of("test", "test"), metricProperties.getMetricTags());
  }

  @Test
  void getMetricTags() throws IllegalAccessException {
    MetricProperties metricProperties = new MetricProperties();
    Map<String, String> tags = Collections.singletonMap("test", "test");
    FieldUtils.writeField(metricProperties, "tags", tags, true);
    assertEquals(Tags.of("test", "test"), metricProperties.getMetricTags());
  }

  @Test
  void countExecution() {
    assertFalse(metricProperties.countExecution());
    metricProperties.getCount().setExecution(true);
    assertTrue(metricProperties.countExecution());
  }

  @Test
  void countFailure() {
    assertFalse(metricProperties.countFailure());
    metricProperties.getCount().setFailure(true);
    assertTrue(metricProperties.countFailure());
  }

  static class MetricProperties extends MetricsProperties {

  }
}
