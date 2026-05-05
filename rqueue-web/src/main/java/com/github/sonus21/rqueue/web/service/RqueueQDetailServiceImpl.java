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

package com.github.sonus21.rqueue.web.service;

import static com.github.sonus21.rqueue.utils.StringUtils.clean;
import static com.google.common.collect.Lists.newArrayList;

import com.github.sonus21.rqueue.config.RqueueConfig;
import com.github.sonus21.rqueue.core.EndpointRegistry;
import com.github.sonus21.rqueue.core.RqueueMessage;
import com.github.sonus21.rqueue.core.RqueueMessageTemplate;
import com.github.sonus21.rqueue.core.spi.MessageBroker;
import com.github.sonus21.rqueue.core.spi.SubscriberView;
import com.github.sonus21.rqueue.core.support.RqueueMessageUtils;
import com.github.sonus21.rqueue.exception.UnknownSwitchCase;
import com.github.sonus21.rqueue.listener.QueueDetail;
import com.github.sonus21.rqueue.models.db.DeadLetterQueue;
import com.github.sonus21.rqueue.models.db.MessageMetadata;
import com.github.sonus21.rqueue.models.db.QueueConfig;
import com.github.sonus21.rqueue.models.enums.ActionType;
import com.github.sonus21.rqueue.models.enums.DataType;
import com.github.sonus21.rqueue.models.enums.NavTab;
import com.github.sonus21.rqueue.models.enums.TableColumnType;
import com.github.sonus21.rqueue.models.registry.RqueueWorkerPollerView;
import com.github.sonus21.rqueue.models.response.Action;
import com.github.sonus21.rqueue.models.response.DataViewResponse;
import com.github.sonus21.rqueue.models.response.RedisDataDetail;
import com.github.sonus21.rqueue.models.response.RowColumnMeta;
import com.github.sonus21.rqueue.models.response.RowColumnMetaType;
import com.github.sonus21.rqueue.models.response.SubscriberRow;
import com.github.sonus21.rqueue.models.response.TableColumn;
import com.github.sonus21.rqueue.models.response.TableRow;
import com.github.sonus21.rqueue.models.response.TerminalStorageRow;
import com.github.sonus21.rqueue.repository.MessageBrowsingRepository;
import com.github.sonus21.rqueue.service.RqueueMessageMetadataService;
import com.github.sonus21.rqueue.utils.Constants;
import com.github.sonus21.rqueue.utils.DateTimeUtils;
import com.github.sonus21.rqueue.utils.StringUtils;
import com.github.sonus21.rqueue.web.RqueueQDetailService;
import com.github.sonus21.rqueue.web.RqueueSystemManagerService;
import com.github.sonus21.rqueue.worker.RqueueWorkerRegistry;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.stream.Collectors;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.DefaultTypedTuple;
import org.springframework.data.redis.core.ZSetOperations.TypedTuple;
import org.springframework.stereotype.Service;
import org.springframework.util.CollectionUtils;
import reactor.core.publisher.Mono;

@Service
public class RqueueQDetailServiceImpl implements RqueueQDetailService {

  private final MessageBrowsingRepository messageBrowsingRepository;
  private final RqueueMessageTemplate rqueueMessageTemplate;
  private final RqueueSystemManagerService rqueueSystemManagerService;
  private final RqueueMessageMetadataService rqueueMessageMetadataService;
  private final RqueueConfig rqueueConfig;
  private final RqueueWorkerRegistry rqueueWorkerRegistry;

  /**
   * Optional broker SPI. When set (non-Redis backend), the dashboard prefers
   * {@link MessageBroker#size(QueueDetail)} and
   * {@link MessageBroker#peek(QueueDetail, long, long)} for read paths instead of
   * the Redis DAOs. {@code @Autowired(required = false)} keeps the Redis-only path
   * unchanged when no broker is configured.
   */
  private MessageBroker messageBroker;

  @Autowired
  public RqueueQDetailServiceImpl(
      MessageBrowsingRepository messageBrowsingRepository,
      RqueueMessageTemplate rqueueMessageTemplate,
      RqueueSystemManagerService rqueueSystemManagerService,
      RqueueMessageMetadataService rqueueMessageMetadataService,
      RqueueConfig rqueueConfig,
      RqueueWorkerRegistry rqueueWorkerRegistry) {
    this.messageBrowsingRepository = messageBrowsingRepository;
    this.rqueueMessageTemplate = rqueueMessageTemplate;
    this.rqueueSystemManagerService = rqueueSystemManagerService;
    this.rqueueMessageMetadataService = rqueueMessageMetadataService;
    this.rqueueConfig = rqueueConfig;
    this.rqueueWorkerRegistry = rqueueWorkerRegistry;
  }

  @Autowired(required = false)
  public void setMessageBroker(MessageBroker messageBroker) {
    this.messageBroker = messageBroker;
  }

  /**
   * Visible for tests and pluggable backends.
   */
  public MessageBroker getMessageBroker() {
    return messageBroker;
  }

  /**
   * Resolves a {@link QueueDetail} for the given queue name. Returns {@code null} when no
   * detail is registered (e.g. shutdown / late init); callers should fall back to the Redis
   * path in that case.
   */
  private QueueDetail lookupQueueDetail(String queueName) {
    try {
      return EndpointRegistry.get(queueName);
    } catch (Exception e) {
      return null;
    }
  }

  private boolean brokerHidesScheduled() {
    return messageBroker != null && !messageBroker.capabilities().supportsScheduledIntrospection();
  }

  private boolean brokerHidesCron() {
    return messageBroker != null && !messageBroker.capabilities().supportsCronJobs();
  }

  /**
   * Returns true when the broker manages its own in-flight tracking and does not use the Redis
   * processing ZSET. Brokers that return {@code usesPrimaryHandlerDispatch() == false} (e.g. NATS)
   * have no separate "running" queue to inspect, so the RUNNING tab/row must be suppressed.
   */
  private boolean brokerHidesRunning() {
    return messageBroker != null && !messageBroker.capabilities().usesPrimaryHandlerDispatch();
  }

  @Override
  public String storageKicker() {
    return messageBroker != null ? messageBroker.storageKicker() : "Redis";
  }

  @Override
  public String storageDescription() {
    return messageBroker != null
        ? messageBroker.storageDescription()
        : "Underlying Redis structures for the queues visible on this page.";
  }

  @Override
  public Map<String, List<Entry<NavTab, RedisDataDetail>>> getQueueDataStructureDetails(
      List<QueueConfig> queueConfig) {
    return queueConfig.parallelStream()
        .collect(Collectors.toMap(QueueConfig::getName, this::getQueueDataStructureDetail));
  }

  @Override
  public List<Entry<NavTab, RedisDataDetail>> getQueueDataStructureDetail(QueueConfig queueConfig) {
    if (queueConfig == null) {
      return Collections.emptyList();
    }
    // Route size lookup through the broker SPI when configured (non-Redis backend).
    QueueDetail brokerQueueDetail =
        messageBroker != null ? lookupQueueDetail(queueConfig.getName()) : null;
    Long pending;
    if (brokerQueueDetail != null) {
      pending = messageBroker.size(brokerQueueDetail);
    } else {
      pending = messageBrowsingRepository.getDataSize(queueConfig.getQueueName(), DataType.LIST);
    }
    // When a non-Redis broker is configured, use its storage display names instead of Redis keys.
    String pendingDisplayName =
        brokerQueueDetail != null && messageBroker.storageDisplayName(brokerQueueDetail) != null
            ? messageBroker.storageDisplayName(brokerQueueDetail)
            : queueConfig.getQueueName();
    List<Entry<NavTab, RedisDataDetail>> queueRedisDataDetails = newArrayList();
    // Per-consumer pending breakdown for brokers that expose it (e.g. NATS Limits-retention
    // streams where each durable consumer has its own offset). When present, render one row
    // per consumer with an exact pending count instead of a single aggregated "~ N" row.
    Map<String, Long> perConsumer =
        brokerQueueDetail != null ? messageBroker.consumerPendingSizes(brokerQueueDetail) : null;
    if (perConsumer != null && !perConsumer.isEmpty()) {
      String label = brokerLabel(NavTab.PENDING, DataType.LIST);
      for (Map.Entry<String, Long> entry : perConsumer.entrySet()) {
        Long size = entry.getValue();
        RedisDataDetail consumerDetail =
            new RedisDataDetail(pendingDisplayName, DataType.LIST, size == null ? 0 : size);
        consumerDetail.setTypeLabel(label);
        consumerDetail.setConsumerName(entry.getKey());
        // Per-consumer counts are exact (numPending or position math for that subscriber);
        // the approximation flag only applies to the aggregated single-row view.
        queueRedisDataDetails.add(new HashMap.SimpleEntry<>(NavTab.PENDING, consumerDetail));
      }
    } else {
      RedisDataDetail pendingDetail =
          new RedisDataDetail(pendingDisplayName, DataType.LIST, pending == null ? 0 : pending);
      pendingDetail.setTypeLabel(brokerLabel(NavTab.PENDING, DataType.LIST));
      if (brokerQueueDetail != null) {
        pendingDetail.setApproximate(messageBroker.isSizeApproximate(brokerQueueDetail));
      }
      queueRedisDataDetails.add(new HashMap.SimpleEntry<>(NavTab.PENDING, pendingDetail));
    }
    // Brokers that manage their own in-flight tracking (e.g. NATS JetStream) have no separate
    // processing ZSET, so omit the RUNNING entry to avoid a 501 when the explorer opens it.
    if (!brokerHidesRunning()) {
      String processingQueueName = queueConfig.getProcessingQueueName();
      Long running = messageBrowsingRepository.getDataSize(processingQueueName, DataType.ZSET);
      RedisDataDetail runningDetail =
          new RedisDataDetail(processingQueueName, DataType.ZSET, running == null ? 0 : running);
      runningDetail.setTypeLabel(brokerLabel(NavTab.RUNNING, DataType.ZSET));
      queueRedisDataDetails.add(new HashMap.SimpleEntry<>(NavTab.RUNNING, runningDetail));
    }
    String scheduledQueueName = queueConfig.getScheduledQueueName();
    // When the broker doesn't support scheduled introspection (e.g. JetStream), suppress
    // the SCHEDULED nav tab entry entirely so the dashboard doesn't query an absent ZSET.
    if (!brokerHidesScheduled()) {
      Long scheduled = messageBrowsingRepository.getDataSize(scheduledQueueName, DataType.ZSET);
      RedisDataDetail scheduledDetail =
          new RedisDataDetail(scheduledQueueName, DataType.ZSET, scheduled == null ? 0 : scheduled);
      scheduledDetail.setTypeLabel(brokerLabel(NavTab.SCHEDULED, DataType.ZSET));
      queueRedisDataDetails.add(new HashMap.SimpleEntry<>(NavTab.SCHEDULED, scheduledDetail));
    }
    if (!CollectionUtils.isEmpty(queueConfig.getDeadLetterQueues())) {
      for (DeadLetterQueue dlq : queueConfig.getDeadLetterQueues()) {
        String dlqDisplayName = brokerQueueDetail != null
                && messageBroker.dlqStorageDisplayName(brokerQueueDetail) != null
            ? messageBroker.dlqStorageDisplayName(brokerQueueDetail)
            : dlq.getName();
        RedisDataDetail dlqDetail;
        if (!dlq.isConsumerEnabled()) {
          Long dlqSize = messageBrowsingRepository.getDataSize(dlq.getName(), DataType.LIST);
          dlqDetail =
              new RedisDataDetail(dlqDisplayName, DataType.LIST, dlqSize == null ? 0 : dlqSize);
        } else {
          // TODO should we redirect to the queue page?
          dlqDetail = new RedisDataDetail(dlqDisplayName, DataType.LIST, -1);
        }
        dlqDetail.setTypeLabel(brokerLabel(NavTab.DEAD, DataType.LIST));
        queueRedisDataDetails.add(new HashMap.SimpleEntry<>(NavTab.DEAD, dlqDetail));
      }
    }
    if (rqueueConfig.messageInTerminalStateShouldBeStored()
        && !StringUtils.isEmpty(queueConfig.getCompletedQueueName())) {
      Long completed =
          messageBrowsingRepository.getDataSize(queueConfig.getCompletedQueueName(), DataType.ZSET);
      String completedDisplayName =
          brokerQueueDetail != null && messageBroker.storageDisplayName(brokerQueueDetail) != null
              ? messageBroker.storageDisplayName(brokerQueueDetail)
              : queueConfig.getCompletedQueueName();
      RedisDataDetail completedDetail = new RedisDataDetail(
          completedDisplayName, DataType.ZSET, completed == null ? 0 : completed);
      completedDetail.setTypeLabel(brokerLabel(NavTab.COMPLETED, DataType.ZSET));
      queueRedisDataDetails.add(new HashMap.SimpleEntry<>(NavTab.COMPLETED, completedDetail));
    }
    return queueRedisDataDetails;
  }

  /**
   * Resolve the broker-specific human-readable label for the given (NavTab, DataType) pair.
   * Returns {@code null} on the legacy Redis path so the template falls back to
   * {@code DataType.name()} (LIST/ZSET).
   */
  private String brokerLabel(NavTab tab, DataType type) {
    return messageBroker != null ? messageBroker.dataTypeLabel(tab, type) : null;
  }

  @Override
  public List<NavTab> getNavTabs(QueueConfig queueConfig) {
    List<NavTab> navTabs = new ArrayList<>();
    if (queueConfig != null) {
      navTabs.add(NavTab.PENDING);
      // Hide SCHEDULED tab for brokers without scheduled-queue introspection support.
      if (!brokerHidesScheduled()) {
        navTabs.add(NavTab.SCHEDULED);
      }
      // Hide RUNNING tab for brokers that manage in-flight tracking internally (e.g. NATS).
      if (!brokerHidesRunning()) {
        navTabs.add(NavTab.RUNNING);
      }
      if (queueConfig.hasDeadLetterQueue()) {
        navTabs.add(NavTab.DEAD);
      }
    }
    return navTabs;
  }

  private List<TypedTuple<RqueueMessage>> readFromZset(
      String name, int pageNumber, int itemPerPage) {
    requireScheduledIntrospection("readFromZset");
    long start = pageNumber * (long) itemPerPage;
    long end = start + itemPerPage - 1;

    return rqueueMessageTemplate.readFromZset(name, start, end).stream()
        .map(e -> new DefaultTypedTuple<>(e, null))
        .collect(Collectors.toList());
  }

  private List<TypedTuple<RqueueMessage>> readFromList(
      String name, int pageNumber, int itemPerPage) {
    long start = pageNumber * (long) itemPerPage;
    long end = start + itemPerPage - 1;
    return rqueueMessageTemplate.readFromList(name, start, end).stream()
        .map(e -> new DefaultTypedTuple<>(e, null))
        .collect(Collectors.toList());
  }

  private List<TypedTuple<RqueueMessage>> readFromZetWithScore(
      String name, int pageNumber, int itemPerPage) {
    requireScheduledIntrospection("readFromZsetWithScore");
    long start = pageNumber * (long) itemPerPage;
    long end = start + itemPerPage - 1;
    return rqueueMessageTemplate.readFromZsetWithScore(name, start, end);
  }

  /**
   * Guard for ZSET-shaped lookups that the redis backend services natively but no other backend
   * does. Backends that report {@code !supportsScheduledIntrospection()} surface a structured 501
   * via {@code BackendCapabilityException} instead of NPE-ing through a Redis-shaped template
   * with no Redis connection.
   */
  private void requireScheduledIntrospection(String op) {
    if (messageBroker != null && !messageBroker.capabilities().supportsScheduledIntrospection()) {
      throw new com.github.sonus21.rqueue.exception.BackendCapabilityException(
          messageBroker.getClass().getSimpleName(),
          op,
          "broker does not expose scheduled / completion ZSET introspection");
    }
  }

  private List<TableRow> buildRows(
      List<TypedTuple<RqueueMessage>> rqueueMessages, RowBuilder rowBuilder) {
    if (CollectionUtils.isEmpty(rqueueMessages)) {
      return Collections.emptyList();
    }
    Map<String, String> messageMetaIdToId = new HashMap<>();
    for (TypedTuple<RqueueMessage> tuple : rqueueMessages) {
      RqueueMessage rqueueMessage = tuple.getValue();
      assert rqueueMessage != null;
      String messageMetaId =
          RqueueMessageUtils.getMessageMetaId(rqueueMessage.getQueueName(), rqueueMessage.getId());
      messageMetaIdToId.put(messageMetaId, rqueueMessage.getId());
    }
    List<MessageMetadata> vals = rqueueMessageMetadataService.findAll(messageMetaIdToId.keySet());
    Map<String, Boolean> msgIdToDeleted = new HashMap<>();
    for (MessageMetadata messageMetadata : vals) {
      String messageMetaId = messageMetadata.getId();
      String id = messageMetaIdToId.get(messageMetaId);
      msgIdToDeleted.put(id, messageMetadata.isDeleted());
    }
    return rqueueMessages.stream()
        .map(e -> rowBuilder.row(
            e.getValue(), msgIdToDeleted.getOrDefault(e.getValue().getId(), false), e.getScore()))
        .collect(Collectors.toList());
  }

  private void addActionsIfRequired(
      String src,
      String name,
      DataType type,
      boolean scheduledQueue,
      boolean deadLetterQueue,
      boolean completionQueue,
      DataViewResponse response) {
    if (deadLetterQueue) {
      response.addAction(
          new Action(ActionType.DELETE, String.format("dead letter queue '%s'", name)));
    } else if (completionQueue) {
      response.addAction(
          new Action(ActionType.DELETE, String.format("Completed messages for queue '%s'", name)));
    } else if (type == DataType.LIST) {
      response.addAction(
          new Action(ActionType.DELETE, String.format("pending messages for queue '%s'", src)));
    } else if (scheduledQueue) {
      response.addAction(
          new Action(ActionType.DELETE, String.format("scheduled messages for queue '%s'", src)));
    }
  }

  @Override
  public DataViewResponse getExplorePageData(
      String src,
      String name,
      DataType type,
      String consumerName,
      int pageNumber,
      int itemPerPage) {
    QueueConfig queueConfig = rqueueSystemManagerService.getQueueConfig(src);
    DataViewResponse response = new DataViewResponse();
    boolean deadLetterQueue = queueConfig.isDeadLetterQueue(name);
    boolean scheduledQueue = queueConfig.getScheduledQueueName().equals(name);
    boolean completionQueue = name.equals(queueConfig.getCompletedQueueName());
    // Surface broker capability hints so the UI can hide the corresponding panels.
    response.setHideScheduledPanel(brokerHidesScheduled());
    response.setHideCronJobs(brokerHidesCron());
    // When the broker does not support scheduled-queue introspection, return an empty
    // result set for the scheduled tab. The hideScheduledPanel flag (above) tells the
    // frontend to grey out / hide the panel.
    if (scheduledQueue && brokerHidesScheduled()) {
      response.setRows(Collections.emptyList());
      return response;
    }
    setHeadersIfRequired(deadLetterQueue, completionQueue, type, response, pageNumber);
    addActionsIfRequired(
        src, name, type, scheduledQueue, deadLetterQueue, completionQueue, response);
    // Prefer broker.peek() for the ready (LIST) queue when a non-Redis broker is configured.
    if (type == DataType.LIST && !deadLetterQueue && messageBroker != null) {
      QueueDetail qd = lookupQueueDetail(queueConfig.getName());
      if (qd != null) {
        long offset = (long) pageNumber * itemPerPage;
        List<RqueueMessage> peeked = messageBroker.peek(qd, consumerName, offset, itemPerPage);
        List<TypedTuple<RqueueMessage>> tuples = peeked.stream()
            .map(m -> (TypedTuple<RqueueMessage>) new DefaultTypedTuple<>(m, null))
            .collect(Collectors.toList());
        response.setRows(buildRows(tuples, new ListRowBuilder(false)));
        return response;
      }
    }
    switch (type) {
      case ZSET:
        if (scheduledQueue) {
          response.setRows(buildRows(
              readFromZset(name, pageNumber, itemPerPage), new ZsetRowBuilder(true, false)));
        } else if (completionQueue) {
          response.setRows(buildRows(
              readFromMessageMetadataStore(name, pageNumber, itemPerPage),
              new ZsetRowBuilder(false, true)));
        } else {
          response.setRows(buildRows(
              readFromZetWithScore(name, pageNumber, itemPerPage),
              new ZsetRowBuilder(false, false)));
        }
        break;
      case LIST:
        response.setRows(buildRows(
            readFromList(name, pageNumber, itemPerPage), new ListRowBuilder(deadLetterQueue)));
        break;
      default:
        throw new UnknownSwitchCase(type.name());
    }
    return response;
  }

  private List<TypedTuple<RqueueMessage>> readFromMessageMetadataStore(
      String name, int pageNumber, int itemPerPage) {
    long start = pageNumber * (long) itemPerPage;
    long end = start + itemPerPage - 1;
    List<TypedTuple<MessageMetadata>> mes =
        rqueueMessageMetadataService.readMessageMetadataForQueue(name, start, end);
    if (CollectionUtils.isEmpty(mes)) {
      return Collections.emptyList();
    }
    return mes.stream()
        .map(e -> new DefaultTypedTuple<>(
            Objects.requireNonNull(e.getValue()).getRqueueMessage(),
            (double) e.getValue().getUpdatedOn()))
        .collect(Collectors.toList());
  }

  @Override
  public DataViewResponse viewData(
      String name, DataType type, String key, int pageNumber, int itemPerPage) {
    if (StringUtils.isEmpty(name)) {
      return DataViewResponse.createErrorMessage("Data name cannot be empty.");
    }
    if (DataType.isUnknown(type)) {
      return DataViewResponse.createErrorMessage("Data type is not provided.");
    }
    // Delegate the per-type dispatch to the storage layer. Backends without arbitrary keyed
    // reads (NATS) throw BackendCapabilityException → 501; the web advice surfaces it.
    return messageBrowsingRepository.viewData(
        clean(name), type, key == null ? null : clean(key), pageNumber, itemPerPage);
  }

  private void setHeadersIfRequired(
      boolean deadLetterQueue,
      boolean completedQueue,
      DataType type,
      DataViewResponse response,
      int pageNumber) {
    if (pageNumber != 0) {
      return;
    }
    List<String> headers = newArrayList("Id", "Message", "Type");
    if (DataType.ZSET == type && !completedQueue) {
      headers.add("Time Left");
    }
    if (deadLetterQueue) {
      headers.add("Added On");
    } else if (completedQueue) {
      headers.add("Completed On");
    } else {
      headers.add("Action");
    }
    response.setHeaders(headers);
  }

  @Override
  public List<List<Object>> getRunningTasks() {
    // Brokers that manage in-flight tracking internally (e.g. NATS JetStream durable consumers)
    // have no separate processing ZSET to report on. Surface an empty table with just the header
    // row so the home dashboard shows the section but doesn't render a column of zeros.
    if (brokerHidesRunning()) {
      return emptyTable("Processing");
    }
    return bulkSizeTable(
        rqueueSystemManagerService.getSortedQueueConfigs(),
        QueueConfig::getProcessingQueueName,
        DataType.ZSET,
        "Processing [ZSET]");
  }

  @Override
  public List<List<Object>> getWaitingTasks() {
    return bulkSizeTable(
        rqueueSystemManagerService.getSortedQueueConfigs(),
        QueueConfig::getQueueName,
        DataType.LIST,
        "Queue [LIST]");
  }

  @Override
  public List<List<Object>> getScheduledTasks() {
    // Brokers without scheduled-queue introspection (e.g. NATS JetStream) have no scheduled ZSET.
    // Return an empty table so the home dashboard doesn't query an absent data structure.
    if (brokerHidesScheduled()) {
      return emptyTable("Scheduled");
    }
    return bulkSizeTable(
        rqueueSystemManagerService.getSortedQueueConfigs(),
        QueueConfig::getScheduledQueueName,
        DataType.ZSET,
        "Scheduled [ZSET]");
  }

  /**
   * Header-only table used when a broker capability suppresses an entire section (e.g.
   * NATS hiding the running / scheduled rows). The frontend renders the column header and
   * no body rows.
   */
  private List<List<Object>> emptyTable(String section) {
    List<List<Object>> rows = new ArrayList<>();
    rows.add(Arrays.asList("Queue", section, "Number of Messages"));
    return rows;
  }

  /**
   * Render the home-dashboard "queue / data-name / count" 3-column table for a per-queue data
   * structure. The repository's {@link MessageBrowsingRepository#getDataSizes(List, List)} is
   * expected to pipeline on Redis; NATS returns zeros.
   */
  private List<List<Object>> bulkSizeTable(
      List<QueueConfig> queueConfigs,
      java.util.function.Function<QueueConfig, String> nameExtractor,
      DataType dataType,
      String columnLabel) {
    List<List<Object>> rows = new ArrayList<>();
    rows.add(Arrays.asList("Queue", columnLabel, "Number of Messages"));
    if (CollectionUtils.isEmpty(queueConfigs)) {
      return rows;
    }
    List<String> names = new ArrayList<>(queueConfigs.size());
    List<DataType> types = new ArrayList<>(queueConfigs.size());
    for (QueueConfig queueConfig : queueConfigs) {
      names.add(nameExtractor.apply(queueConfig));
      types.add(dataType);
    }
    List<Long> sizes = messageBrowsingRepository.getDataSizes(names, types);
    for (int i = 0; i < queueConfigs.size(); i++) {
      rows.add(Arrays.asList(queueConfigs.get(i).getName(), names.get(i), sizes.get(i)));
    }
    return rows;
  }

  private void addRows(
      List<Long> result,
      List<List<Object>> rows,
      List<Entry<QueueConfig, String>> queueConfigAndDlq) {
    for (int i = 0, j = 0; i < queueConfigAndDlq.size(); i++) {
      Entry<QueueConfig, String> entry = queueConfigAndDlq.get(i);
      QueueConfig queueConfig = entry.getKey();
      if (entry.getValue().isEmpty()) {
        rows.add(Arrays.asList(queueConfig.getName(), Constants.BLANK, Constants.BLANK));
      } else {
        String name = Constants.BLANK;
        if (i == 0
            || !queueConfig
                .getQueueName()
                .equals(queueConfigAndDlq.get(i - 1).getKey().getQueueName())) {
          name = queueConfig.getName();
        }
        rows.add(Arrays.asList(name, entry.getValue(), result.get(j++)));
      }
    }
  }

  @Override
  public List<List<Object>> getDeadLetterTasks() {
    List<QueueConfig> queueConfigs = rqueueSystemManagerService.getSortedQueueConfigs();
    List<Entry<QueueConfig, String>> queueConfigAndDlq = new ArrayList<>();
    for (QueueConfig queueConfig : queueConfigs) {
      if (queueConfig.hasDeadLetterQueue()) {
        for (DeadLetterQueue dlq : queueConfig.getDeadLetterQueues()) {
          queueConfigAndDlq.add(new HashMap.SimpleEntry<>(queueConfig, dlq.getName()));
        }
      } else {
        queueConfigAndDlq.add(new HashMap.SimpleEntry<>(queueConfig, Constants.BLANK));
      }
    }
    List<List<Object>> rows = new ArrayList<>();
    List<Long> result = new ArrayList<>();
    if (!CollectionUtils.isEmpty(queueConfigAndDlq)) {
      List<String> dlqNames = new ArrayList<>();
      List<DataType> dlqTypes = new ArrayList<>();
      for (Entry<QueueConfig, String> entry : queueConfigAndDlq) {
        if (!entry.getValue().isEmpty()) {
          dlqNames.add(entry.getValue());
          dlqTypes.add(DataType.LIST);
        }
      }
      result = messageBrowsingRepository.getDataSizes(dlqNames, dlqTypes);
    }
    rows.add(Arrays.asList("Queue", "Dead Letter Queues [LIST]", "Number of Messages"));
    addRows(result, rows, queueConfigAndDlq);
    return rows;
  }

  @Override
  public List<RqueueWorkerPollerView> getQueueWorkers(String queueName) {
    return rqueueWorkerRegistry.getQueueWorkers(queueName);
  }

  // -------------------------------------------------------------------------
  // Subscriber + Terminal Storage rows (new queue-detail UI)
  // -------------------------------------------------------------------------

  /**
   * Build the per-subscriber rows that drive the new "Subscribers" section. Joins broker SPI
   * data ({@link MessageBroker#subscribers}) with last-active info from the worker registry,
   * keyed on {@code consumerName}. Falls back to a single anonymous row when the queue has
   * no registered handlers (e.g. a producer-only deployment).
   */
  @Override
  public List<SubscriberRow> getSubscriberRows(QueueConfig queueConfig) {
    if (queueConfig == null) {
      return Collections.emptyList();
    }
    QueueDetail brokerQueueDetail =
        messageBroker != null ? lookupQueueDetail(queueConfig.getName()) : null;
    List<SubscriberView> views = brokerSubscribers(queueConfig, brokerQueueDetail);
    if (views.isEmpty()) {
      return Collections.emptyList();
    }
    String storageName =
        brokerQueueDetail != null && messageBroker.storageDisplayName(brokerQueueDetail) != null
            ? messageBroker.storageDisplayName(brokerQueueDetail)
            : queueConfig.getQueueName();
    String label = brokerLabel(NavTab.PENDING, DataType.LIST);
    Map<String, RqueueWorkerPollerView> workersByConsumer =
        indexWorkersByConsumer(queueConfig.getName());
    List<SubscriberRow> rows = new ArrayList<>(views.size());
    long now = System.currentTimeMillis();
    for (SubscriberView v : views) {
      SubscriberRow.SubscriberRowBuilder builder = SubscriberRow.builder()
          .consumerName(v.consumerName())
          .typeLabel(label)
          .storageName(storageName)
          .dataType(DataType.LIST)
          .pending(v.pending())
          .pendingShared(v.pendingShared())
          .inFlight(v.inFlight());
      RqueueWorkerPollerView w = workersByConsumer.get(v.consumerName());
      if (w != null) {
        builder
            .status(w.getStatus())
            .host(w.getHost())
            .pid(w.getPid())
            .lastPollAt(w.getLastPollAt());
        if (w.getLastPollAt() > 0) {
          builder.lastPollAge(DateTimeUtils.milliToHumanRepresentation(now - w.getLastPollAt()));
        }
      }
      rows.add(builder.build());
    }
    return rows;
  }

  private List<SubscriberView> brokerSubscribers(
      QueueConfig queueConfig, QueueDetail brokerQueueDetail) {
    if (brokerQueueDetail != null && messageBroker != null) {
      try {
        List<SubscriberView> views = messageBroker.subscribers(brokerQueueDetail);
        if (views != null && !views.isEmpty()) {
          return views;
        }
      } catch (RuntimeException ignored) {
        // fall through to producer-only path
      }
    }
    // No active QueueDetail registered (producer-only or shutdown). Surface a single row so
    // the operator at least sees the queue's pending count from the repository fallback.
    Long pending = messageBrowsingRepository.getDataSize(queueConfig.getQueueName(), DataType.LIST);
    if (pending == null || pending <= 0) {
      return Collections.emptyList();
    }
    return Collections.singletonList(new SubscriberView(queueConfig.getName(), pending, 0L, true));
  }

  private Map<String, RqueueWorkerPollerView> indexWorkersByConsumer(String queueName) {
    List<RqueueWorkerPollerView> workers = rqueueWorkerRegistry.getQueueWorkers(queueName);
    if (CollectionUtils.isEmpty(workers)) {
      return Collections.emptyMap();
    }
    Map<String, RqueueWorkerPollerView> out = new HashMap<>(workers.size());
    for (RqueueWorkerPollerView w : workers) {
      String key = w.getConsumerName();
      if (key == null || key.isEmpty()) {
        continue;
      }
      RqueueWorkerPollerView existing = out.get(key);
      if (existing == null || w.getLastPollAt() > existing.getLastPollAt()) {
        out.put(key, w);
      }
    }
    return out;
  }

  /**
   * Terminal-storage rows: COMPLETED set + each DLQ. These are shared across subscribers, so
   * they live in their own table rather than being repeated on every subscriber row.
   */
  @Override
  public List<TerminalStorageRow> getTerminalRows(QueueConfig queueConfig) {
    if (queueConfig == null) {
      return Collections.emptyList();
    }
    List<TerminalStorageRow> out = new ArrayList<>();
    QueueDetail brokerQueueDetail =
        messageBroker != null ? lookupQueueDetail(queueConfig.getName()) : null;
    if (rqueueConfig.messageInTerminalStateShouldBeStored()
        && !StringUtils.isEmpty(queueConfig.getCompletedQueueName())) {
      Long completed =
          messageBrowsingRepository.getDataSize(queueConfig.getCompletedQueueName(), DataType.ZSET);
      String completedDisplayName =
          brokerQueueDetail != null && messageBroker.storageDisplayName(brokerQueueDetail) != null
              ? messageBroker.storageDisplayName(brokerQueueDetail)
              : queueConfig.getCompletedQueueName();
      out.add(TerminalStorageRow.builder()
          .tab(NavTab.COMPLETED)
          .typeLabel(brokerLabel(NavTab.COMPLETED, DataType.ZSET))
          .storageName(completedDisplayName)
          .dataType(DataType.ZSET)
          .size(completed == null ? 0L : completed)
          .build());
    }
    if (!CollectionUtils.isEmpty(queueConfig.getDeadLetterQueues())) {
      for (DeadLetterQueue dlq : queueConfig.getDeadLetterQueues()) {
        String dlqDisplayName = brokerQueueDetail != null
                && messageBroker.dlqStorageDisplayName(brokerQueueDetail) != null
            ? messageBroker.dlqStorageDisplayName(brokerQueueDetail)
            : dlq.getName();
        long size;
        if (dlq.isConsumerEnabled()) {
          size = -1L;
        } else {
          Long dlqSize = messageBrowsingRepository.getDataSize(dlq.getName(), DataType.LIST);
          size = dlqSize == null ? 0L : dlqSize;
        }
        out.add(TerminalStorageRow.builder()
            .tab(NavTab.DEAD)
            .typeLabel(brokerLabel(NavTab.DEAD, DataType.LIST))
            .storageName(dlqDisplayName)
            .dataType(DataType.LIST)
            .size(size)
            .build());
      }
    }
    return out;
  }

  @Override
  public Mono<DataViewResponse> getReactiveExplorePageData(
      String src,
      String name,
      DataType type,
      String consumerName,
      int pageNumber,
      int itemPerPage) {
    return Mono.just(getExplorePageData(src, name, type, consumerName, pageNumber, itemPerPage));
  }

  @Override
  public Mono<DataViewResponse> viewReactiveData(
      String name, DataType type, String key, int pageNumber, int itemPerPage) {
    return Mono.just(viewData(name, type, key, pageNumber, itemPerPage));
  }

  private interface RowBuilder {

    default TableRow getRow(RqueueMessage rqueueMessage) {
      TableRow row = new TableRow(new TableColumn(rqueueMessage.getId()));
      TableColumn column = new TableColumn(rqueueMessage.toString());
      column.setMeta(Collections.singletonList(
          new RowColumnMeta(RowColumnMetaType.JOBS_BUTTON, rqueueMessage.getId())));
      row.addColumn(column);
      if (rqueueMessage.isPeriodic()) {
        row.addColumn(new TableColumn("Periodic(" + rqueueMessage.getPeriod() + ")Ms"));
      } else {
        row.addColumn(new TableColumn("Simple"));
      }
      return row;
    }

    TableRow row(RqueueMessage rqueueMessage, boolean deleted, Double score);
  }

  private static class ListRowBuilder implements RowBuilder {

    private final boolean deadLetterQueue;

    ListRowBuilder(boolean deadLetterQueue) {
      this.deadLetterQueue = deadLetterQueue;
    }

    @Override
    public TableRow row(RqueueMessage rqueueMessage, boolean deleted, Double score) {
      TableRow tableRow = getRow(rqueueMessage);
      if (!deadLetterQueue) {
        if (deleted) {
          tableRow.addColumn(new TableColumn(Constants.BLANK));
        } else {
          tableRow.addColumn(new TableColumn(TableColumnType.ACTION, ActionType.DELETE));
        }
      } else {
        tableRow.addColumn(
            new TableColumn(DateTimeUtils.formatMilliToString(rqueueMessage.getReEnqueuedAt())));
      }
      return tableRow;
    }
  }

  private static class ZsetRowBuilder implements RowBuilder {

    private final long currentTime;
    private final boolean scheduledQueue;
    private final boolean completionQueue;

    ZsetRowBuilder(boolean scheduledQueue, boolean completionQueue) {
      this.scheduledQueue = scheduledQueue;
      this.completionQueue = completionQueue;
      this.currentTime = System.currentTimeMillis();
    }

    @Override
    public TableRow row(RqueueMessage rqueueMessage, boolean deleted, Double score) {
      TableRow row = getRow(rqueueMessage);
      if (scheduledQueue) {
        row.addColumn(new TableColumn(
            DateTimeUtils.milliToHumanRepresentation(rqueueMessage.getProcessAt() - currentTime)));
      } else if (completionQueue) {
        row.addColumn(new TableColumn(DateTimeUtils.milliToHumanRepresentation(
            System.currentTimeMillis() - score.longValue())));
      } else {
        row.addColumn(new TableColumn(
            DateTimeUtils.milliToHumanRepresentation(score.longValue() - currentTime)));
      }
      if (!completionQueue) {
        if (!deleted) {
          row.addColumn(new TableColumn(
              TableColumnType.ACTION,
              scheduledQueue && !rqueueMessage.isPeriodic()
                  ? ActionType.ENQUEUE
                  : ActionType.DELETE));
        } else {
          row.addColumn(new TableColumn(Constants.BLANK));
        }
      }
      return row;
    }
  }
}
