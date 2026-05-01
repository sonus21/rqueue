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

package com.github.sonus21.rqueue.redis.repository;

import com.github.sonus21.rqueue.common.RqueueRedisTemplate;
import com.github.sonus21.rqueue.exception.UnknownSwitchCase;
import com.github.sonus21.rqueue.models.enums.DataType;
import com.github.sonus21.rqueue.models.response.DataViewResponse;
import com.github.sonus21.rqueue.models.response.TableColumn;
import com.github.sonus21.rqueue.models.response.TableRow;
import com.github.sonus21.rqueue.repository.MessageBrowsingRepository;
import com.github.sonus21.rqueue.utils.RedisUtils;
import com.github.sonus21.rqueue.utils.StringUtils;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.springframework.data.redis.core.ZSetOperations.TypedTuple;

/**
 * Redis-backed {@link MessageBrowsingRepository}. Wraps {@link RqueueRedisTemplate} for the
 * size and range primitives; delegates raw {@link org.springframework.data.redis.core.RedisTemplate}
 * access for the bulk pipelining used by {@link #getDataSizes} (one round-trip for N queues
 * instead of N).
 *
 * <p>This class exists to keep the Redis-shaped storage calls behind a stable interface so the
 * single {@code RqueueQDetailServiceImpl} in {@code rqueue-web} can serve both backends.
 */
public class RedisMessageBrowsingRepository implements MessageBrowsingRepository {

  private final RqueueRedisTemplate<String> stringTemplate;

  public RedisMessageBrowsingRepository(RqueueRedisTemplate<String> stringTemplate) {
    this.stringTemplate = stringTemplate;
  }

  @Override
  public long getDataSize(String name, DataType type) {
    Long size;
    switch (type) {
      case LIST:
        size = stringTemplate.getListSize(name);
        break;
      case ZSET:
        size = stringTemplate.getZsetSize(name);
        break;
      default:
        // SET / KEY sizes are not used by the dashboard; return 0 rather than throw.
        return 0L;
    }
    return size == null ? 0L : size;
  }

  @Override
  public List<Long> getDataSizes(List<String> names, List<DataType> types) {
    if (names == null || names.isEmpty()) {
      return Collections.emptyList();
    }
    if (types == null || names.size() != types.size()) {
      throw new IllegalArgumentException(
          "names and types must be the same length; names=" + names.size()
              + " types=" + (types == null ? "null" : types.size()));
    }
    List<Object> raw = RedisUtils.executePipeLine(
        stringTemplate.getRedisTemplate(),
        (connection, keySerializer, valueSerializer) -> {
          for (int i = 0; i < names.size(); i++) {
            byte[] key = keySerializer.serialize(names.get(i));
            switch (types.get(i)) {
              case LIST:
                connection.lLen(key);
                break;
              case ZSET:
                connection.zCard(key);
                break;
              default:
                // Unknown size: emit something to keep pipeline alignment with the input.
                connection.exists(key);
            }
          }
        });
    List<Long> out = new ArrayList<>(names.size());
    for (Object o : raw) {
      if (o instanceof Number) {
        out.add(((Number) o).longValue());
      } else {
        out.add(0L);
      }
    }
    return out;
  }

  @Override
  public DataViewResponse viewData(
      String name, DataType type, String key, int pageNumber, int itemPerPage) {
    switch (type) {
      case SET:
        return responseForSet(name);
      case ZSET:
        return responseForZset(name, key, pageNumber, itemPerPage);
      case LIST:
        return responseForList(name, pageNumber, itemPerPage);
      case KEY:
        return responseForKeyVal(name);
      default:
        throw new UnknownSwitchCase(type.name());
    }
  }

  private DataViewResponse responseForSet(String name) {
    List<Object> items = new ArrayList<>(stringTemplate.getMembers(name));
    DataViewResponse response = new DataViewResponse();
    response.setHeaders(Collections.singletonList("Item"));
    List<TableRow> tableRows = new ArrayList<>();
    for (Object item : items) {
      tableRows.add(new TableRow(new TableColumn(item.toString())));
    }
    response.setRows(tableRows);
    return response;
  }

  private DataViewResponse responseForKeyVal(String name) {
    DataViewResponse response = new DataViewResponse();
    response.setHeaders(Collections.singletonList("Value"));
    Object val = stringTemplate.get(name);
    response.addRow(new TableRow(new TableColumn(String.valueOf(val))));
    return response;
  }

  private DataViewResponse responseForZset(
      String name, String key, int pageNumber, int itemPerPage) {
    DataViewResponse response = new DataViewResponse();
    int start = pageNumber * itemPerPage;
    int end = start + itemPerPage - 1;
    List<TableRow> tableRows = new ArrayList<>();
    if (!StringUtils.isEmpty(key)) {
      Double score = stringTemplate.getZsetMemberScore(name, key);
      response.setHeaders(Collections.singletonList("Score"));
      tableRows.add(new TableRow(new TableColumn(score)));
    } else {
      response.setHeaders(Arrays.asList("Value", "Score"));
      for (TypedTuple<String> tuple : stringTemplate.zrangeWithScore(name, start, end)) {
        tableRows.add(new TableRow(Arrays.asList(
            new TableColumn(String.valueOf(tuple.getValue())), new TableColumn(tuple.getScore()))));
      }
    }
    response.setRows(tableRows);
    return response;
  }

  private DataViewResponse responseForList(String name, int pageNumber, int itemPerPage) {
    DataViewResponse response = new DataViewResponse();
    response.setHeaders(Collections.singletonList("Item"));
    int start = pageNumber * itemPerPage;
    int end = start + itemPerPage - 1;
    List<TableRow> tableRows = new ArrayList<>();
    for (Object s : stringTemplate.lrange(name, start, end)) {
      tableRows.add(new TableRow(new TableColumn(String.valueOf(s))));
    }
    response.setRows(tableRows);
    return response;
  }
}
