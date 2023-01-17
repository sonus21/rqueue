/*
 * Copyright (c) 2021-2023 Sonu Kumar
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

package com.github.sonus21.rqueue.spring.boot.tests.unit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import com.github.sonus21.TestBase;
import com.github.sonus21.rqueue.metrics.RqueueMetricsRegistry;
import com.github.sonus21.rqueue.spring.boot.RqueueMetricsAutoConfig;
import com.github.sonus21.rqueue.spring.boot.RqueueMetricsProperties;
import com.github.sonus21.rqueue.spring.boot.tests.SpringBootUnitTest;
import io.micrometer.core.instrument.Tags;
import java.util.Collections;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.junit.jupiter.api.Test;
import org.springframework.boot.actuate.autoconfigure.metrics.MetricsProperties;

@SpringBootUnitTest
@Slf4j
class RqueueMetricsAutoConfigTest extends TestBase {

  @Test
  void rqueueMetricsRegistryNoAdditionalTags() {
    RqueueMetricsAutoConfig rqueueMetricsAutoConfig = new RqueueMetricsAutoConfig();
    MetricsProperties metricsProperties = new MetricsProperties();
    RqueueMetricsProperties rqueueMetricsProperties = new RqueueMetricsProperties();
    RqueueMetricsRegistry rqueueMetricsRegistry =
        rqueueMetricsAutoConfig.rqueueMetricsRegistry(metricsProperties, rqueueMetricsProperties);
    assertNotNull(rqueueMetricsRegistry);
    assertEquals(Tags.empty(), rqueueMetricsProperties.getMetricTags());
  }

  @Test
  void rqueueMetricsRegistryTagsFromRqueueProperties() {
    RqueueMetricsAutoConfig rqueueMetricsAutoConfig = new RqueueMetricsAutoConfig();
    MetricsProperties metricsProperties = new MetricsProperties();
    RqueueMetricsProperties rqueueMetricsProperties = new RqueueMetricsProperties();
    rqueueMetricsProperties.setTags(Collections.singletonMap("dc", "test"));
    RqueueMetricsRegistry rqueueMetricsRegistry =
        rqueueMetricsAutoConfig.rqueueMetricsRegistry(metricsProperties, rqueueMetricsProperties);
    assertNotNull(rqueueMetricsRegistry);
    assertEquals(Tags.of("dc", "test"), rqueueMetricsProperties.getMetricTags());
  }

  @Test
  void rqueueMetricsRegistryMergedTags() throws IllegalAccessException {
    RqueueMetricsAutoConfig rqueueMetricsAutoConfig = new RqueueMetricsAutoConfig();
    MetricsProperties metricsProperties = new MetricsProperties();
    try {
      MetricsProperties.class.getMethod("getTags");
    } catch (Exception e) {
      log.info("This test is not applicable");
      return;
    }
    RqueueMetricsProperties rqueueMetricsProperties = new RqueueMetricsProperties();
    rqueueMetricsProperties.setTags(Collections.singletonMap("dc", "test"));
    FieldUtils.writeField(
        metricsProperties, "tags", Collections.singletonMap("region", "ap-south-1"), true);
    RqueueMetricsRegistry rqueueMetricsRegistry =
        rqueueMetricsAutoConfig.rqueueMetricsRegistry(metricsProperties, rqueueMetricsProperties);
    assertNotNull(rqueueMetricsRegistry);
    assertEquals(
        Tags.of("region", "ap-south-1", "dc", "test"), rqueueMetricsProperties.getMetricTags());
  }
}
