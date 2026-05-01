/*
 * Copyright (c) 2024-2026 Sonu Kumar
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

package com.github.sonus21.rqueue.nats.repository;

import com.github.sonus21.rqueue.exception.BackendCapabilityException;
import com.github.sonus21.rqueue.models.enums.DataType;
import com.github.sonus21.rqueue.models.response.DataViewResponse;
import com.github.sonus21.rqueue.repository.MessageBrowsingRepository;
import java.util.ArrayList;
import java.util.List;

/**
 * NATS-backend impl of {@link MessageBrowsingRepository}. JetStream KV doesn't expose the
 * positional list / sorted-set primitives the dashboard's data-explorer panel requires, so
 * {@link #viewData} throws {@link BackendCapabilityException} (mapped to HTTP 501 by the web
 * advice). The size queries return {@code 0} — total in-flight / pending counts on a NATS
 * backend are surfaced through {@code MessageBroker.size(QueueDetail)} elsewhere; the raw
 * dashboard counts here represent Redis-data-structure sizes that have no NATS counterpart.
 */
public class NatsMessageBrowsingRepository implements MessageBrowsingRepository {

  @Override
  public long getDataSize(String name, DataType type) {
    return 0L;
  }

  @Override
  public List<Long> getDataSizes(List<String> names, List<DataType> types) {
    if (names == null || names.isEmpty()) {
      return new ArrayList<>();
    }
    List<Long> out = new ArrayList<>(names.size());
    for (int i = 0; i < names.size(); i++) {
      out.add(0L);
    }
    return out;
  }

  @Override
  public DataViewResponse viewData(
      String name, DataType type, String key, int pageNumber, int itemPerPage) {
    throw new BackendCapabilityException(
        "nats",
        "viewData",
        "JetStream does not expose positional reads on arbitrary keys; the dashboard's"
            + " data-explorer panel is Redis-only in v1.");
  }
}
