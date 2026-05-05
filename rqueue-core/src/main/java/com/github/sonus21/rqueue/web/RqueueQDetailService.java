/*
 * Copyright (c) 2020-2026 Sonu Kumar
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

package com.github.sonus21.rqueue.web;

import com.github.sonus21.rqueue.models.db.QueueConfig;
import com.github.sonus21.rqueue.models.enums.DataType;
import com.github.sonus21.rqueue.models.enums.NavTab;
import com.github.sonus21.rqueue.models.registry.RqueueWorkerPollerView;
import com.github.sonus21.rqueue.models.response.DataViewResponse;
import com.github.sonus21.rqueue.models.response.RedisDataDetail;
import com.github.sonus21.rqueue.models.response.SubscriberRow;
import com.github.sonus21.rqueue.models.response.TerminalStorageRow;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import reactor.core.publisher.Mono;

public interface RqueueQDetailService {

  String storageKicker();

  String storageDescription();

  Map<String, List<Entry<NavTab, RedisDataDetail>>> getQueueDataStructureDetails(
      List<QueueConfig> queueConfig);

  List<Entry<NavTab, RedisDataDetail>> getQueueDataStructureDetail(QueueConfig queueConfig);

  /**
   * One row per subscriber attached to the queue (every {@code @RqueueListener} on Redis,
   * every JetStream durable consumer on NATS). Backs the new "Subscribers" section on the
   * queue-detail page; folds in last-active info from the worker registry so the standalone
   * Queue Pollers section can be retired.
   */
  List<SubscriberRow> getSubscriberRows(QueueConfig queueConfig);

  /**
   * One row per terminal storage bucket (COMPLETED, DEAD letter queues) for the given queue.
   * These are not per-consumer — they are shared stores — so the dashboard renders them in a
   * separate section from the per-subscriber rows.
   */
  List<TerminalStorageRow> getTerminalRows(QueueConfig queueConfig);

  List<NavTab> getNavTabs(QueueConfig queueConfig);

  DataViewResponse getExplorePageData(
      String src, String name, DataType type, int pageNumber, int itemPerPage);

  DataViewResponse viewData(
      String name, DataType type, String key, int pageNumber, int itemPerPage);

  List<List<Object>> getRunningTasks();

  List<List<Object>> getWaitingTasks();

  List<List<Object>> getScheduledTasks();

  List<List<Object>> getDeadLetterTasks();

  List<RqueueWorkerPollerView> getQueueWorkers(String queueName);

  Mono<DataViewResponse> getReactiveExplorePageData(
      String src, String name, DataType type, int pageNumber, int itemPerPage);

  Mono<DataViewResponse> viewReactiveData(
      String name, DataType type, String key, int pageNumber, int itemPerPage);
}
