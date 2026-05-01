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

package com.github.sonus21.rqueue.repository;

import com.github.sonus21.rqueue.models.enums.DataType;
import com.github.sonus21.rqueue.models.response.DataViewResponse;
import java.util.List;

/**
 * Backend-shaped browsing primitives consumed by the dashboard's queue-detail and data-explorer
 * pages. Replaces direct {@code RqueueRedisTemplate} access from
 * {@code RqueueQDetailServiceImpl}, allowing a single backend-neutral service impl in
 * {@code rqueue-web}.
 *
 * <p>The interface deliberately exposes Redis-shaped operations on arbitrary keys (LIST / ZSET /
 * SET / KEY); it is the storage-layer abstraction, not a feature contract. Backends without an
 * equivalent (e.g. NATS JetStream KV) throw {@code BackendCapabilityException} from
 * {@link #viewData} and return {@code 0} from the size queries; the dashboard either hides the
 * panel (via capability flags) or surfaces the 501 cleanly.
 */
public interface MessageBrowsingRepository {

  /**
   * Size of a single data structure addressed by {@code name}. Returns {@code 0} when the key
   * does not exist or the backend cannot model the structure.
   */
  long getDataSize(String name, DataType type);

  /**
   * Bulk size — same semantics as {@link #getDataSize} per element, but Redis impls are
   * expected to pipeline the round-trips. {@code names} and {@code types} must be the same
   * length; the returned list is the same length and order.
   */
  List<Long> getDataSizes(List<String> names, List<DataType> types);

  /**
   * Raw data-explorer browser used by the dashboard's "Data" page. Reads paginated rows out of
   * a backing data structure (LIST / ZSET / SET) or returns a single keyed value (KEY / single
   * ZSET member score). Backends without arbitrary keyed reads throw
   * {@code BackendCapabilityException}.
   *
   * @param name the storage key to read.
   * @param type the structure type at {@code name}.
   * @param key  optional sub-key (e.g. ZSET member name to fetch a single score). Empty / null
   *             returns a paginated range.
   * @param pageNumber zero-based page index.
   * @param itemPerPage page size.
   */
  DataViewResponse viewData(
      String name, DataType type, String key, int pageNumber, int itemPerPage);
}
