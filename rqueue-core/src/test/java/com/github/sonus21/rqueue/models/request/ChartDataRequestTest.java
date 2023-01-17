/*
 * Copyright (c) 2020-2023 Sonu Kumar
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

package com.github.sonus21.rqueue.models.request;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;

import com.github.sonus21.TestBase;
import com.github.sonus21.rqueue.CoreUnitTest;
import com.github.sonus21.rqueue.config.RqueueWebConfig;
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

  @Test
  void numberOfDays() {
    RqueueWebConfig rqueueWebConfig = mock(RqueueWebConfig.class);
    doReturn(180).when(rqueueWebConfig).getHistoryDay();
    ChartDataRequest chartDataRequest = new ChartDataRequest();

    chartDataRequest.setAggregationType(AggregationType.DAILY);
    assertEquals(180, chartDataRequest.numberOfDays(rqueueWebConfig));

    chartDataRequest.setNumber(200);
    assertEquals(180, chartDataRequest.numberOfDays(rqueueWebConfig));

    chartDataRequest.setNumber(-6);
    assertEquals(180, chartDataRequest.numberOfDays(rqueueWebConfig));

    chartDataRequest.setAggregationType(AggregationType.WEEKLY);
    assertEquals(180, chartDataRequest.numberOfDays(rqueueWebConfig));

    chartDataRequest.setNumber(10);
    assertEquals(70, chartDataRequest.numberOfDays(rqueueWebConfig));

    chartDataRequest.setNumber(100);
    assertEquals(180, chartDataRequest.numberOfDays(rqueueWebConfig));

    chartDataRequest.setAggregationType(AggregationType.MONTHLY);
    assertEquals(180, chartDataRequest.numberOfDays(rqueueWebConfig));

    chartDataRequest.setNumber(-1);
    assertEquals(180, chartDataRequest.numberOfDays(rqueueWebConfig));

    chartDataRequest.setNumber(2);
    assertEquals(60, chartDataRequest.numberOfDays(rqueueWebConfig));
  }
}
