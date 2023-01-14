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

import com.github.sonus21.rqueue.config.RqueueWebConfig;
import com.github.sonus21.rqueue.models.SerializableBase;
import com.github.sonus21.rqueue.models.enums.AggregationType;
import com.github.sonus21.rqueue.models.enums.ChartDataType;
import com.github.sonus21.rqueue.models.enums.ChartType;
import com.github.sonus21.rqueue.models.response.ChartDataResponse;
import com.github.sonus21.rqueue.utils.Constants;
import java.util.List;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.ToString;

@Getter
@Setter
@NoArgsConstructor
@ToString
public class ChartDataRequest extends SerializableBase {

  private static final long serialVersionUID = 7727090378318819986L;
  private ChartType type;
  private String queue;
  private int number;
  private AggregationType aggregationType;
  private List<ChartDataType> dateTypes;

  public ChartDataRequest(ChartType chartType, AggregationType aggregationType) {
    this.type = chartType;
    this.aggregationType = aggregationType;
  }

  public int numberOfDays(RqueueWebConfig rqueueWebConfig) {
    int n = this.number;
    if (aggregationType == AggregationType.MONTHLY) {
      n = this.number * Constants.DAYS_IN_A_MONTH;
    } else if (aggregationType == AggregationType.WEEKLY) {
      n = this.number * Constants.DAYS_IN_A_WEEK;
    }
    if (n <= 0 || n > rqueueWebConfig.getHistoryDay()) {
      return rqueueWebConfig.getHistoryDay();
    }
    return n;
  }

  public ChartDataResponse validate() {
    ChartDataResponse chartDataResponse = new ChartDataResponse();
    if (getType() == null) {
      chartDataResponse.set(1, "type cannot be null");
      return chartDataResponse;
    }
    if (getAggregationType() == null) {
      chartDataResponse.set(1, "aggregationType cannot be null");
      return chartDataResponse;
    }
    return null;
  }
}
