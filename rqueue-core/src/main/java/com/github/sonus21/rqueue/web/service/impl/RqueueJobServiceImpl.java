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

package com.github.sonus21.rqueue.web.service.impl;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.sonus21.rqueue.dao.RqueueJobDao;
import com.github.sonus21.rqueue.exception.ProcessingException;
import com.github.sonus21.rqueue.models.db.CheckinMessage;
import com.github.sonus21.rqueue.models.db.RqueueJob;
import com.github.sonus21.rqueue.models.response.DataViewResponse;
import com.github.sonus21.rqueue.models.response.TableColumn;
import com.github.sonus21.rqueue.models.response.TableRow;
import com.github.sonus21.rqueue.utils.Constants;
import com.github.sonus21.rqueue.utils.DateTimeUtils;
import com.github.sonus21.rqueue.utils.SerializationUtils;
import com.github.sonus21.rqueue.utils.StringUtils;
import com.github.sonus21.rqueue.web.service.RqueueJobService;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.util.CollectionUtils;
import reactor.core.publisher.Mono;

@Service
public class RqueueJobServiceImpl implements RqueueJobService {

  private final RqueueJobDao rqueueJobDao;
  private final ObjectMapper objectMapper;

  @Autowired
  public RqueueJobServiceImpl(RqueueJobDao rqueueJobDao) {
    this.rqueueJobDao = rqueueJobDao;
    this.objectMapper = SerializationUtils.createObjectMapper();
  }

  private TableRow getTableRow(RqueueJob job) throws ProcessingException {
    List<TableColumn> columns = new LinkedList<>();
    columns.add(new TableColumn(job.getId()));
    columns.add(
        new TableColumn(
            String.format(
                "%s/%s",
                DateTimeUtils.formatMilliToString(job.getCreatedAt()),
                DateTimeUtils.formatMilliToString(job.getUpdatedAt()))));
    if (job.getLastCheckinAt() == 0) {
      columns.add(new TableColumn(""));
    } else {
      long timeDifference = System.currentTimeMillis() - job.getLastCheckinAt();
      columns.add(
          new TableColumn(DateTimeUtils.milliToHumanRepresentation(timeDifference) + " Ago"));
    }
    if (!StringUtils.isEmpty(job.getError())) {
      columns.add(new TableColumn(job.getError()));
    } else {
      columns.add(new TableColumn(Constants.BLANK));
    }
    columns.add(new TableColumn(job.getStatus()));
    List<CheckinMessage> checkinMessages = job.getCheckins();
    if (CollectionUtils.isEmpty(checkinMessages)) {
      columns.add(new TableColumn(Constants.BLANK));
    } else {
      try {
        String data = objectMapper.writeValueAsString(job.getCheckins());
        columns.add(new TableColumn(data));
      } catch (JsonProcessingException e) {
        throw new ProcessingException(e.getMessage(), e);
      }
    }
    return new TableRow(columns);
  }

  @Override
  public DataViewResponse getJobs(String messageId) throws ProcessingException {
    List<RqueueJob> jobList = rqueueJobDao.finByMessageId(messageId);
    DataViewResponse response = new DataViewResponse();
    if (jobList.isEmpty()) {
      response.setCode(0);
      response.setMessage("No jobs found");
    } else {
      jobList.sort(
          (o1, o2) -> {
            long diff = o1.getUpdatedAt() - o2.getUpdatedAt();
            if (diff == 0) {
              return 0;
            }
            if (diff > 0) {
              return 1;
            }
            return -1;
          });
      response.setHeaders(
          Arrays.asList("Id", "StartTime/EndTime", "Last Checkin", "Error", "Status", "CheckIns"));
      for (RqueueJob job : jobList) {
        response.addRow(getTableRow(job));
      }
    }
    return response;
  }

  @Override
  public Mono<DataViewResponse> getReactiveJobs(String messageId) throws ProcessingException {
    return Mono.just(getJobs(messageId));
  }
}
