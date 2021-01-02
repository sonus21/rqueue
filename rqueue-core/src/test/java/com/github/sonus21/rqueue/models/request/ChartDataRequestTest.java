/*
 * Copyright 2020 Sonu Kumar
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.github.sonus21.rqueue.models.request;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import com.github.sonus21.TestBase;
import com.github.sonus21.rqueue.CoreUnitTest;
import com.github.sonus21.rqueue.models.enums.AggregationType;
import com.github.sonus21.rqueue.models.enums.ChartDataType;
import com.github.sonus21.rqueue.models.enums.ChartType;
import com.github.sonus21.rqueue.models.response.ChartDataResponse;
import java.util.Collections;
import org.junit.jupiter.api.Test;

@CoreUnitTest
class ChartDataRequestTest extends TestBase {

  @Test
  void validate() {
    ChartDataRequest chartDataRequest = new ChartDataRequest();
    ChartDataResponse response = chartDataRequest.validate();
    assertEquals(1, response.getCode());
    assertEquals("type cannot be null", response.getMessage());

    chartDataRequest.setType(ChartType.LATENCY);
    response = chartDataRequest.validate();
    assertEquals(1, response.getCode());
    assertEquals("aggregationType cannot be null", response.getMessage());

    chartDataRequest.setAggregationType(AggregationType.MONTHLY);
    chartDataRequest.setDateTypes(Collections.singletonList(ChartDataType.EXECUTION));
    assertNull(chartDataRequest.validate());
  }
}
