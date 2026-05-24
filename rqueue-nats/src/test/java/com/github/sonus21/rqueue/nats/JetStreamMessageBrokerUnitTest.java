/*
 * Copyright (c) 2026 Sonu Kumar
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * You may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 */
package com.github.sonus21.rqueue.nats;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.github.sonus21.rqueue.core.RqueueMessage;
import com.github.sonus21.rqueue.core.spi.Capabilities;
import com.github.sonus21.rqueue.core.spi.SubscriberView;
import com.github.sonus21.rqueue.enums.QueueType;
import com.github.sonus21.rqueue.listener.QueueDetail;
import com.github.sonus21.rqueue.models.enums.DataType;
import com.github.sonus21.rqueue.models.enums.NavTab;
import com.github.sonus21.rqueue.nats.internal.NatsProvisioner;
import com.github.sonus21.rqueue.nats.js.JetStreamMessageBroker;
import com.github.sonus21.rqueue.serdes.RqJacksonSerDes;
import com.github.sonus21.rqueue.serdes.SerializationUtils;
import io.nats.client.Connection;
import io.nats.client.Dispatcher;
import io.nats.client.JetStream;
import io.nats.client.JetStreamApiException;
import io.nats.client.JetStreamManagement;
import io.nats.client.JetStreamSubscription;
import io.nats.client.Message;
import io.nats.client.MessageHandler;
import io.nats.client.PullSubscribeOptions;
import io.nats.client.api.ConsumerInfo;
import io.nats.client.api.PublishAck;
import io.nats.client.api.RetentionPolicy;
import io.nats.client.api.SequenceInfo;
import io.nats.client.api.StreamConfiguration;
import io.nats.client.api.StreamInfo;
import io.nats.client.api.StreamState;
import io.nats.client.impl.Headers;
import java.io.IOException;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import reactor.test.StepVerifier;

/**
 * Non-container unit tests for {@link JetStreamMessageBroker} that mock the underlying NATS
 * primitives. These tests target subject naming, pub/sub plumbing, and exception wrapping —
 * end-to-end JetStream behavior is covered by the Docker-gated ITs.
 */
@NatsUnitTest
class JetStreamMessageBrokerUnitTest {

  private static QueueDetail queueNamed(String name) {
    QueueDetail q = mock(QueueDetail.class);
    when(q.getName()).thenReturn(name);
    when(q.getType()).thenReturn(QueueType.QUEUE);
    when(q.resolvedConsumerName()).thenReturn(name + "-consumer");
    return q;
  }

  private static QueueDetail queueNamed(
      String name, long visibilityTimeout, int numRetry, String consumerName) {
    QueueDetail q = queueNamed(name);
    when(q.getVisibilityTimeout()).thenReturn(visibilityTimeout);
    when(q.getNumRetry()).thenReturn(numRetry);
    when(q.resolvedConsumerName()).thenReturn(consumerName);
    return q;
  }

  private static StreamInfo streamInfo(long firstSeq, long lastSeq, long msgCount) {
    return streamInfo(firstSeq, lastSeq, msgCount, RetentionPolicy.WorkQueue);
  }

  private static StreamInfo streamInfo(
      long firstSeq, long lastSeq, long msgCount, RetentionPolicy retentionPolicy) {
    StreamState state = mock(StreamState.class);
    when(state.getFirstSequence()).thenReturn(firstSeq);
    when(state.getLastSequence()).thenReturn(lastSeq);
    when(state.getMsgCount()).thenReturn(msgCount);
    StreamConfiguration configuration = mock(StreamConfiguration.class);
    when(configuration.getRetentionPolicy()).thenReturn(retentionPolicy);
    StreamInfo info = mock(StreamInfo.class);
    when(info.getStreamState()).thenReturn(state);
    when(info.getConfiguration()).thenReturn(configuration);
    return info;
  }

  private static ConsumerInfo consumerInfo(
      long pending, long ackPending, long ackFloorSequence, long deliveredSequence) {
    ConsumerInfo info = mock(ConsumerInfo.class);
    when(info.getNumPending()).thenReturn(pending);
    when(info.getNumAckPending()).thenReturn(ackPending);
    SequenceInfo ackFloor = mock(SequenceInfo.class);
    when(ackFloor.getStreamSequence()).thenReturn(ackFloorSequence);
    when(info.getAckFloor()).thenReturn(ackFloor);
    SequenceInfo delivered = mock(SequenceInfo.class);
    when(delivered.getStreamSequence()).thenReturn(deliveredSequence);
    when(info.getDelivered()).thenReturn(delivered);
    return info;
  }

  /** Build a broker with all NATS primitives mocked and stream provisioning short-circuited. */
  private static Fixture newFixture(RqueueNatsConfig config) {
    return newFixture(config, false);
  }

  /** Build a broker with {@code schedulingSupported} controlled by the caller. */
  private static Fixture newFixture(RqueueNatsConfig config, boolean schedulingSupported) {
    Connection conn = mock(Connection.class);
    JetStream js = mock(JetStream.class);
    JetStreamManagement jsm = mock(JetStreamManagement.class);
    // Mock the provisioner so ensureStream() is a no-op — these tests verify subject
    // naming and exception wrapping, not stream creation.
    NatsProvisioner provisioner = mock(NatsProvisioner.class);
    when(provisioner.isMessageSchedulingSupported()).thenReturn(schedulingSupported);
    JetStreamMessageBroker broker = new JetStreamMessageBroker(
        conn,
        js,
        jsm,
        config,
        new RqJacksonSerDes(SerializationUtils.getObjectMapper()),
        provisioner);
    return new Fixture(conn, js, jsm, provisioner, broker);
  }

  @Test
  void enqueue_publishesToPrefixedSubject() throws Exception {
    Fixture f = newFixture(RqueueNatsConfig.defaults());
    when(f.js.publish(any(String.class), any(Headers.class), any(byte[].class)))
        .thenReturn(mock(PublishAck.class));
    f.broker.enqueue(
        queueNamed("orders"), RqueueMessage.builder().id("m1").message("hi").build());
    verify(f.js, times(1)).publish(eq("rqueue.js.orders"), any(Headers.class), any(byte[].class));
  }

  @Test
  void getPollWait_usesConfiguredFetchWait() {
    RqueueNatsConfig cfg = RqueueNatsConfig.defaults().setDefaultFetchWait(Duration.ofSeconds(9));
    Fixture f = newFixture(cfg);

    assertEquals(Duration.ofSeconds(9), f.broker.getPollWait(Duration.ofMillis(137)));
  }

  @Test
  void getPollWait_fallsBackToPollingIntervalWhenFetchWaitIsUnset() {
    RqueueNatsConfig cfg = RqueueNatsConfig.defaults().setDefaultFetchWait(null);
    Fixture f = newFixture(cfg);
    Duration pollingInterval = Duration.ofMillis(137);

    assertEquals(pollingInterval, f.broker.getPollWait(pollingInterval));
  }

  @Test
  void popPriorityBindsConsumerWithQueueRetrySettingsAndTracksInFlightMessage() throws Exception {
    Fixture f = newFixture(RqueueNatsConfig.defaults());
    QueueDetail q = queueNamed("orders", 2_500L, 4, "worker-a");
    JetStreamSubscription subscription = mock(JetStreamSubscription.class);
    when(f.provisioner.ensureConsumer(
            eq("rqueue-js-orders_high"),
            eq("worker-a"),
            eq("rqueue.js.orders_high"),
            eq(Duration.ofMillis(2_500L)),
            eq(5L),
            anyLong()))
        .thenReturn("worker-a");
    when(f.js.subscribe(eq(null), any(PullSubscribeOptions.class))).thenReturn(subscription);
    RqueueMessage message = RqueueMessage.builder().id("m1").message("payload").build();
    Message natsMessage = mock(Message.class);
    when(natsMessage.getData())
        .thenReturn(new RqJacksonSerDes(SerializationUtils.getObjectMapper()).serialize(message));
    when(subscription.fetch(eq(1), eq(Duration.ofMillis(50L)))).thenReturn(List.of(natsMessage));

    List<RqueueMessage> messages = f.broker.pop(q, "high", "worker-a", 1, Duration.ZERO);

    assertEquals(1, messages.size());
    assertEquals("m1", messages.get(0).getId());
    assertTrue(f.broker.extendVisibilityTimeout(q, messages.get(0), 1_000L));
    verify(natsMessage, times(1)).inProgress();
    assertTrue(f.broker.nack(q, messages.get(0), -1L));
    verify(natsMessage, times(1)).nakWithDelay(Duration.ZERO);
    assertFalse(f.broker.ack(q, messages.get(0)));
  }

  @Test
  void popNaksUndeserializablePayloadAndReturnsRemainingMessages() throws Exception {
    Fixture f = newFixture(RqueueNatsConfig.defaults());
    JetStreamSubscription subscription = mock(JetStreamSubscription.class);
    when(f.provisioner.ensureConsumer(
            eq("rqueue-js-orders"),
            eq("orders-consumer"),
            eq("rqueue.js.orders"),
            any(Duration.class),
            anyLong(),
            anyLong()))
        .thenReturn("orders-consumer");
    when(f.js.subscribe(eq(null), any(PullSubscribeOptions.class))).thenReturn(subscription);
    Message badMessage = mock(Message.class);
    when(badMessage.getData()).thenReturn("not-json".getBytes(UTF_8));
    when(subscription.fetch(eq(1), eq(Duration.ofMillis(10L)))).thenReturn(List.of(badMessage));

    List<RqueueMessage> messages =
        f.broker.pop(queueNamed("orders"), "orders-consumer", 1, Duration.ofMillis(10L));

    assertTrue(messages.isEmpty());
    verify(badMessage, times(1)).nak();
  }

  @Test
  void enqueueWithPriority_appendsPrioritySuffixToSubject() throws Exception {
    Fixture f = newFixture(RqueueNatsConfig.defaults());
    when(f.js.publish(any(String.class), any(Headers.class), any(byte[].class)))
        .thenReturn(mock(PublishAck.class));
    f.broker.enqueue(
        queueNamed("orders"),
        "high",
        RqueueMessage.builder().id("m1").message("hi").build());
    verify(f.js, times(1))
        .publish(eq("rqueue.js.orders_high"), any(Headers.class), any(byte[].class));
  }

  @Test
  void enqueueWithEmptyPriority_fallsBackToUnsuffixedSubject() throws Exception {
    Fixture f = newFixture(RqueueNatsConfig.defaults());
    when(f.js.publish(any(String.class), any(Headers.class), any(byte[].class)))
        .thenReturn(mock(PublishAck.class));
    f.broker.enqueue(
        queueNamed("orders"), "", RqueueMessage.builder().id("m1").message("hi").build());
    verify(f.js, times(1)).publish(eq("rqueue.js.orders"), any(Headers.class), any(byte[].class));
  }

  @Test
  void enqueue_honorsCustomSubjectPrefix() throws Exception {
    RqueueNatsConfig cfg = RqueueNatsConfig.defaults().setSubjectPrefix("custom.");
    Fixture f = newFixture(cfg);
    when(f.js.publish(any(String.class), any(Headers.class), any(byte[].class)))
        .thenReturn(mock(PublishAck.class));
    f.broker.enqueue(
        queueNamed("orders"), RqueueMessage.builder().id("m1").message("hi").build());
    verify(f.js, times(1)).publish(eq("custom.orders"), any(Headers.class), any(byte[].class));
  }

  @Test
  void enqueue_wrapsIoExceptionInRqueueNatsException() throws Exception {
    Fixture f = newFixture(RqueueNatsConfig.defaults());
    when(f.js.publish(any(String.class), any(Headers.class), any(byte[].class)))
        .thenThrow(new IOException("boom"));
    RqueueNatsException ex = assertThrows(
        RqueueNatsException.class,
        () -> f.broker.enqueue(
            queueNamed("orders"), RqueueMessage.builder().id("m1").message("hi").build()));
    assertNotNull(ex.getCause());
  }

  @Test
  void enqueue_wrapsJetStreamApiExceptionInRqueueNatsException() throws Exception {
    Fixture f = newFixture(RqueueNatsConfig.defaults());
    when(f.js.publish(any(String.class), any(Headers.class), any(byte[].class)))
        .thenThrow(mock(JetStreamApiException.class));
    assertThrows(
        RqueueNatsException.class,
        () -> f.broker.enqueue(
            queueNamed("orders"), RqueueMessage.builder().id("m1").message("hi").build()));
  }

  @Test
  void publish_writesUtf8BytesToConnection() {
    Fixture f = newFixture(RqueueNatsConfig.defaults());
    f.broker.publish("chan-1", "hello");
    verify(f.conn, times(1)).publish("chan-1", "hello".getBytes(UTF_8));
  }

  @Test
  void subscribe_createsDispatcherAndSubscribesChannel_closeReleasesIt() throws Exception {
    Fixture f = newFixture(RqueueNatsConfig.defaults());
    Dispatcher d = mock(Dispatcher.class);
    when(f.conn.createDispatcher(any(MessageHandler.class))).thenReturn(d);
    when(d.subscribe(any(String.class))).thenReturn(d);

    AutoCloseable closer = f.broker.subscribe("chan-1", payload -> {});
    verify(f.conn, times(1)).createDispatcher(any(MessageHandler.class));
    verify(d, times(1)).subscribe("chan-1");
    verify(f.conn, never()).closeDispatcher(any());

    closer.close();
    verify(f.conn, times(1)).closeDispatcher(d);
  }

  @Test
  void sizeUsesStreamCountForWorkQueueAndApproximatesLimitsFromSlowestConsumer() throws Exception {
    Fixture f = newFixture(RqueueNatsConfig.defaults());
    StreamInfo workInfo = streamInfo(1L, 9L, 7L);
    when(f.jsm.getStreamInfo("rqueue-js-work")).thenReturn(workInfo);
    assertEquals(7L, f.broker.size(queueNamed("work")));
    assertFalse(f.broker.isSizeApproximate(queueNamed("work")));

    StreamInfo fanoutInfo = streamInfo(1L, 10L, 10L, RetentionPolicy.Limits);
    ConsumerInfo fastConsumer = consumerInfo(1L, 0L, 8L, 9L);
    ConsumerInfo slowConsumer = consumerInfo(5L, 2L, 3L, 4L);
    when(f.jsm.getStreamInfo("rqueue-js-fanout")).thenReturn(fanoutInfo);
    when(f.jsm.getConsumerNames("rqueue-js-fanout")).thenReturn(List.of("fast", "slow"));
    when(f.jsm.getConsumerInfo("rqueue-js-fanout", "fast")).thenReturn(fastConsumer);
    when(f.jsm.getConsumerInfo("rqueue-js-fanout", "slow")).thenReturn(slowConsumer);

    assertEquals(7L, f.broker.size(queueNamed("fanout")));
    assertTrue(f.broker.isSizeApproximate(queueNamed("fanout")));
  }

  @Test
  void subscribersExposeSharedPendingForWorkQueueAndPerConsumerPendingForLimits() throws Exception {
    Fixture f = newFixture(RqueueNatsConfig.defaults());
    StreamInfo workInfo = streamInfo(1L, 4L, 4L);
    ConsumerInfo workConsumer = consumerInfo(12L, 3L, 1L, 1L);
    when(f.jsm.getStreamInfo("rqueue-js-work")).thenReturn(workInfo);
    when(f.jsm.getConsumerNames("rqueue-js-work")).thenReturn(List.of("a"));
    when(f.jsm.getConsumerInfo("rqueue-js-work", "a")).thenReturn(workConsumer);

    List<SubscriberView> workSubscribers = f.broker.subscribers(queueNamed("work"));

    assertEquals(1, workSubscribers.size());
    assertEquals("a", workSubscribers.get(0).consumerName());
    assertEquals(4L, workSubscribers.get(0).pending());
    assertEquals(3L, workSubscribers.get(0).inFlight());
    assertTrue(workSubscribers.get(0).pendingShared());

    StreamInfo fanoutInfo = streamInfo(1L, 6L, 6L, RetentionPolicy.Limits);
    ConsumerInfo fanoutA = consumerInfo(2L, 1L, 3L, 4L);
    ConsumerInfo fanoutB = consumerInfo(5L, 0L, 1L, 1L);
    when(f.jsm.getStreamInfo("rqueue-js-fanout")).thenReturn(fanoutInfo);
    when(f.jsm.getConsumerNames("rqueue-js-fanout")).thenReturn(List.of("a", "b"));
    when(f.jsm.getConsumerInfo("rqueue-js-fanout", "a")).thenReturn(fanoutA);
    when(f.jsm.getConsumerInfo("rqueue-js-fanout", "b")).thenReturn(fanoutB);

    List<SubscriberView> fanoutSubscribers = f.broker.subscribers(queueNamed("fanout"));

    assertEquals(2, fanoutSubscribers.size());
    assertEquals(2L, fanoutSubscribers.get(0).pending());
    assertEquals(1L, fanoutSubscribers.get(0).inFlight());
    assertFalse(fanoutSubscribers.get(0).pendingShared());
    assertEquals(5L, fanoutSubscribers.get(1).pending());
  }

  @Test
  void storageDisplayAndDataTypeLabelsUseNatsTerminology() {
    Fixture f = newFixture(RqueueNatsConfig.defaults());
    QueueDetail q = queueNamed("orders");

    assertEquals("NATS", f.broker.storageKicker());
    assertTrue(f.broker.storageDescription().contains("NATS JetStream"));
    assertEquals("rqueue-js-orders", f.broker.storageDisplayName(q));
    assertEquals("rqueue-js-orders-dlq", f.broker.dlqStorageDisplayName(q));
    assertEquals("Queue (Stream)", f.broker.dataTypeLabel(NavTab.PENDING, DataType.LIST));
    assertEquals("Dead Letter (Stream)", f.broker.dataTypeLabel(NavTab.DEAD, DataType.ZSET));
    assertEquals("Completed (KV)", f.broker.dataTypeLabel(NavTab.COMPLETED, DataType.SET));
    assertEquals("Stream", f.broker.dataTypeLabel(NavTab.RUNNING, DataType.ZSET));
    assertEquals("Stream", f.broker.dataTypeLabel(null, DataType.LIST));
    assertNull(f.broker.dataTypeLabel(null, null));
  }

  @Test
  void enqueueReactive_completesWhenPublishFutureCompletes() {
    Fixture f = newFixture(RqueueNatsConfig.defaults());
    PublishAck ack = mock(PublishAck.class);
    CompletableFuture<PublishAck> done = CompletableFuture.completedFuture(ack);
    when(f.js.publishAsync(any(String.class), any(Headers.class), any(byte[].class)))
        .thenReturn(done);

    StepVerifier.create(f.broker.enqueueReactive(
            queueNamed("orders"), RqueueMessage.builder().id("m1").message("hi").build()))
        .verifyComplete();
    verify(f.js, times(1))
        .publishAsync(eq("rqueue.js.orders"), any(Headers.class), any(byte[].class));
  }

  @Test
  void enqueueReactive_wrapsAsyncFailureInRqueueNatsException() {
    Fixture f = newFixture(RqueueNatsConfig.defaults());
    CompletableFuture<PublishAck> failed = new CompletableFuture<>();
    failed.completeExceptionally(new IOException("network down"));
    when(f.js.publishAsync(any(String.class), any(Headers.class), any(byte[].class)))
        .thenReturn(failed);

    StepVerifier.create(f.broker.enqueueReactive(
            queueNamed("orders"), RqueueMessage.builder().id("m1").message("hi").build()))
        .expectError(RqueueNatsException.class)
        .verify();
  }

  @Test
  void enqueueWithDelayReactive_returnsErrorMonoWhenSchedulingUnsupported() {
    // provisioner mock returns false for isMessageSchedulingSupported() by default
    Fixture f = newFixture(RqueueNatsConfig.defaults());
    StepVerifier.create(f.broker.enqueueWithDelayReactive(
            queueNamed("orders"), RqueueMessage.builder().id("m1").message("hi").build(), 100))
        .expectError(RqueueNatsException.class)
        .verify();
  }

  @Test
  void enqueueWithDelay_periodicMessage_dedupKeyIsIdAtProcessAt() throws Exception {
    // Nats-Msg-Id = id@processAt for periodic messages:
    //   - consecutive periods have unique processAt → different keys → no cross-period suppression
    //   - retries share the same processAt → same key → JetStream deduplicates double scheduleNext
    Fixture f = newFixture(RqueueNatsConfig.defaults(), true);
    when(f.js.publish(any(String.class), any(Headers.class), any(byte[].class)))
        .thenReturn(mock(io.nats.client.api.PublishAck.class));

    long processAt = 1_746_789_000_000L;
    RqueueMessage m = RqueueMessage.builder()
        .id("pid-1")
        .message("hi")
        .processAt(processAt)
        .period(5_000L)
        .build();
    f.broker.enqueueWithDelay(queueNamed("orders"), m, 5_000L);

    ArgumentCaptor<Headers> headersCaptor = ArgumentCaptor.forClass(Headers.class);
    verify(f.js, times(1)).publish(any(String.class), headersCaptor.capture(), any(byte[].class));
    assertEquals("pid-1-at-" + processAt, headersCaptor.getValue().getFirst("Nats-Msg-Id"));
    assertEquals(String.valueOf(processAt), headersCaptor.getValue().getFirst("Rqueue-Process-At"));
    assertEquals("5000", headersCaptor.getValue().getFirst("Rqueue-Period"));
  }

  @Test
  void enqueueWithDelay_nonPeriodicMessage_dedupKeyIsIdAtProcessAt() throws Exception {
    // Nats-Msg-Id = id@processAt for non-periodic scheduled messages too.
    // Double-publish of the same logical message within DuplicateWindow (= ackWait) is
    // deduplicated; after the window expires the same ID may be reused freely.
    Fixture f = newFixture(RqueueNatsConfig.defaults(), true);
    when(f.js.publish(any(String.class), any(Headers.class), any(byte[].class)))
        .thenReturn(mock(io.nats.client.api.PublishAck.class));

    long processAt = 1_746_789_000_000L;
    RqueueMessage m =
        RqueueMessage.builder().id("oid-1").message("hi").processAt(processAt).build();
    f.broker.enqueueWithDelay(queueNamed("orders"), m, 3_000L);

    ArgumentCaptor<Headers> headersCaptor = ArgumentCaptor.forClass(Headers.class);
    verify(f.js, times(1)).publish(any(String.class), headersCaptor.capture(), any(byte[].class));
    assertEquals("oid-1-at-" + processAt, headersCaptor.getValue().getFirst("Nats-Msg-Id"));
    assertEquals(String.valueOf(processAt), headersCaptor.getValue().getFirst("Rqueue-Process-At"));
    assertEquals(null, headersCaptor.getValue().getFirst("Rqueue-Period"));
  }

  // ---- capabilities -----------------------------------------------------

  @Test
  void capabilities_schedulingUnsupported_supportsDelayedEnqueueIsFalse() {
    Fixture f = newFixture(RqueueNatsConfig.defaults(), false);
    Capabilities caps = f.broker.capabilities();
    assertFalse(caps.supportsDelayedEnqueue(), "no scheduling on old NATS");
    assertFalse(caps.supportsScheduledIntrospection(), "no scheduled zset in NATS");
    assertFalse(caps.supportsCronJobs(), "no server-side cron in NATS");
    assertFalse(caps.usesPrimaryHandlerDispatch(), "no Redis processing-ZSET in NATS");
    assertTrue(caps.supportsViewData(), "peek() is implemented — explore panel must be visible");
    assertTrue(caps.supportsMoveMessage(), "NatsRqueueUtilityService.moveMessage() is implemented");
  }

  @Test
  void capabilities_schedulingSupported_supportsDelayedEnqueueIsTrue() {
    Fixture f = newFixture(RqueueNatsConfig.defaults(), true);
    Capabilities caps = f.broker.capabilities();
    assertTrue(caps.supportsDelayedEnqueue());
    assertTrue(caps.supportsViewData());
    assertTrue(caps.supportsMoveMessage());
    assertFalse(caps.usesPrimaryHandlerDispatch());
  }

  // ---- scheduleNext (SPI default) ----------------------------------------

  /**
   * JetStreamMessageBroker does not override {@code scheduleNext}; the SPI default calls
   * {@code enqueueWithDelay(q, message, delay)}. Verify that path reaches JetStream publish
   * to the scheduler subject ({@code <workSubject>.sched.<msgId>}) with the correct ADR-51 headers:
   * {@code Nats-Schedule: @at <RFC3339>}, {@code Nats-Schedule-Target: <workSubject>}, and
   * {@code Nats-Rollup: sub}.
   */
  @Test
  void scheduleNext_delegatesToEnqueueWithDelay_setsScheduleHeaders() throws Exception {
    Fixture f = newFixture(RqueueNatsConfig.defaults(), true);
    when(f.js.publish(any(String.class), any(Headers.class), any(byte[].class)))
        .thenReturn(mock(PublishAck.class));

    long processAt = System.currentTimeMillis() + 5_000L;
    RqueueMessage m = RqueueMessage.builder()
        .id("pid-1")
        .message("payload")
        .processAt(processAt)
        .period(5_000L)
        .build();

    // scheduleNext default: delayMs = max(0, processAt - now) → enqueueWithDelay
    f.broker.scheduleNext(queueNamed("orders"), "ignored-key", m, 60L);

    ArgumentCaptor<String> subject = ArgumentCaptor.forClass(String.class);
    ArgumentCaptor<Headers> headers = ArgumentCaptor.forClass(Headers.class);
    verify(f.js, times(1)).publish(subject.capture(), headers.capture(), any(byte[].class));

    // Must publish to the scheduler subject, NOT the work subject
    assertEquals(
        "rqueue.js.orders.sched.pid-1",
        subject.getValue(),
        "Must publish to scheduler subject <workSubject>.sched.<msgId>");

    // Nats-Schedule header must carry the @at prefix and an RFC3339 UTC time
    String schedule = headers.getValue().getFirst(JetStreamMessageBroker.HDR_SCHEDULE);
    assertNotNull(schedule, "Nats-Schedule header must be present");
    assertTrue(schedule.startsWith("@at "), "Nats-Schedule value must start with '@at '");

    // Nats-Schedule-Target must point to the work subject
    assertEquals(
        "rqueue.js.orders",
        headers.getValue().getFirst(JetStreamMessageBroker.HDR_SCHEDULE_TARGET),
        "Nats-Schedule-Target must be the work subject");

    // Nats-Rollup must be 'sub' for per-subject idempotent scheduling
    assertEquals("sub", headers.getValue().getFirst("Nats-Rollup"), "Nats-Rollup must be 'sub'");

    // Dedup key must encode the period identity
    assertEquals(
        "pid-1-at-" + processAt,
        headers.getValue().getFirst("Nats-Msg-Id"),
        "dedup key must be id-at-processAt");
  }

  // ---- helper -----------------------------------------------------------

  private static final class Fixture {
    final Connection conn;
    final JetStream js;
    final JetStreamManagement jsm;
    final NatsProvisioner provisioner;
    final JetStreamMessageBroker broker;

    Fixture(
        Connection conn,
        JetStream js,
        JetStreamManagement jsm,
        NatsProvisioner provisioner,
        JetStreamMessageBroker broker) {
      this.conn = conn;
      this.js = js;
      this.jsm = jsm;
      this.provisioner = provisioner;
      this.broker = broker;
    }
  }
}
