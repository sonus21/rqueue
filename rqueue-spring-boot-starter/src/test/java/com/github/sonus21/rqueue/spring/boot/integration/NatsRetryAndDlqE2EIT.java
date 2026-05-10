/*
 * Copyright (c) 2026 Sonu Kumar
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
package com.github.sonus21.rqueue.spring.boot.integration;

import static org.assertj.core.api.Assertions.assertThat;

import com.github.sonus21.rqueue.annotation.RqueueListener;
import com.github.sonus21.rqueue.core.RqueueMessageEnqueuer;
import io.nats.client.Connection;
import io.nats.client.JetStreamManagement;
import io.nats.client.api.StreamInfo;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.data.redis.autoconfigure.DataRedisAutoConfiguration;
import org.springframework.boot.data.redis.autoconfigure.DataRedisReactiveAutoConfiguration;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.stereotype.Component;

/**
 * Verifies the NATS-native dead-letter advisory bridge installed by
 * {@link com.github.sonus21.rqueue.nats.js.NatsDeadLetterBridgeRegistrar}: when JetStream emits
 * {@code $JS.EVENT.ADVISORY.CONSUMER.MAX_DELIVERIES.<stream>.<consumer>} for a queue's durable
 * consumer, the registrar's dispatcher looks up the offending message by sequence number and
 * republishes it onto the queue's DLQ stream ({@code <streamPrefix><queue><dlqStreamSuffix>}).
 *
 * <p><b>Why a synthetic advisory.</b> In normal rqueue flow the framework either acks
 * (success / forced-discard / moveToDlq) or naks (retry) every delivery, so NATS sees a terminal
 * action before {@code maxDeliver} elapses and the advisory never fires from a real handler — that
 * path is covered by
 * {@code NatsSchedulingAdvancedE2EIT#scheduledMessageExhaustsRetriesToDlq} via the rqueue-level
 * DLQ ({@code PostProcessingHandler.moveToDlq}). The advisory bridge is a defensive net for cases
 * outside rqueue's control (process crash mid-handler, or a handler that blocks past its
 * visibility timeout AND past every retry while NATS keeps redelivering). Triggering that path
 * end-to-end is racy and slow, so this test instead publishes a synthetic advisory matching the
 * shape {@code nats-server 2.12} actually emits and asserts the dispatcher reacts: enqueues a
 * payload, looks up its stream sequence, fakes the advisory, and waits for the DLQ stream to
 * receive it.
 */
@SpringBootTest(
    classes = NatsRetryAndDlqE2EIT.TestApp.class,
    properties = {
      "rqueue.backend=nats",
      "rqueue.nats.naming.stream-prefix=" + NatsRetryAndDlqE2EIT.STREAM_PREFIX,
      "rqueue.nats.naming.subject-prefix=" + NatsRetryAndDlqE2EIT.SUBJECT_PREFIX
    })
@Tag("nats")
class NatsRetryAndDlqE2EIT extends AbstractNatsBootIT {

  static final String STREAM_PREFIX = "rqueue-js-retryDlqE2E-";
  static final String SUBJECT_PREFIX = "rqueue.js.retryDlqE2E.";

  @BeforeAll
  static void wipeOwnedStreams() {
    deleteStreamsWithPrefix(STREAM_PREFIX);
  }

  @Autowired
  RqueueMessageEnqueuer enqueuer;

  @Autowired
  Connection natsConnection;

  @Autowired
  JetStreamManagement jsm;

  @Autowired
  BlockingListener listener;

  @Test
  void advisoryBridgeRepublishesIntoDlqStream() throws Exception {
    String stream = STREAM_PREFIX + "boom";
    String dlqStream = stream + "-dlq";

    // Publish via the normal enqueue path so the source stream + bridge get provisioned exactly
    // the way they would in production. The blocking listener picks up the delivery but never
    // returns, so the message stays in flight (un-acked) in the WorkQueue stream — which is the
    // realistic state when JetStream actually fires the max-delivery advisory in production
    // (handler hung, ack never sent). The long visibilityTimeout keeps NATS from redelivering
    // during the test.
    enqueuer.enqueue("boom", "marker-payload");

    // Confirm the listener has the message in flight (proves the source stream still has it: in
    // WorkQueue retention an unacked message stays in the stream until acked or AckWait expires).
    assertThat(listener.received.await(20, TimeUnit.SECONDS))
        .as("Listener must receive the marker payload before we synthesize the advisory")
        .isTrue();

    // Bridge installer provisioned the DLQ stream from boot (installDeadLetterBridge →
    // provisionDlq).
    Awaitility.await().atMost(Duration.ofSeconds(5)).untilAsserted(() -> {
      assertThat(jsm.getStreamInfo(dlqStream)).isNotNull();
    });
    long sourceSeq = jsm.getStreamInfo(stream).getStreamState().getLastSequence();

    // Synthetic max-delivery advisory matching nats-server 2.12's payload shape: only stream_seq
    // is required by the bridge's republish logic. The advisory subject must include
    // <stream>.<consumer>; consumer name is the rqueue default for this queue, which is
    // <queueName>-consumer (see QueueDetail#resolvedConsumerName).
    String consumer = "boom-consumer";
    String advisorySubject =
        "$JS.EVENT.ADVISORY.CONSUMER.MAX_DELIVERIES." + stream + "." + consumer;
    String advisoryJson = "{\"type\":\"io.nats.jetstream.advisory.v1.max_deliveries\","
        + "\"stream\":\"" + stream + "\","
        + "\"consumer\":\"" + consumer + "\","
        + "\"stream_seq\":" + sourceSeq + ","
        + "\"deliveries\":3}";
    natsConnection.publish(advisorySubject, advisoryJson.getBytes(StandardCharsets.UTF_8));
    natsConnection.flush(Duration.ofSeconds(2));

    // The bridge's dispatcher should have republished the source message onto the DLQ stream.
    Awaitility.await().atMost(Duration.ofSeconds(15)).untilAsserted(() -> {
      StreamInfo dlq = jsm.getStreamInfo(dlqStream);
      assertThat(dlq.getStreamState().getMsgCount()).isGreaterThanOrEqualTo(1);
    });
  }

  @SpringBootApplication(
      exclude = {DataRedisAutoConfiguration.class, DataRedisReactiveAutoConfiguration.class})
  @Import(BlockingListener.class)
  static class TestApp {}

  /**
   * Receives the marker payload, signals the test, then blocks past the test's runtime so the
   * message stays in flight (un-acked) in the WorkQueue source stream. That keeps it reachable via
   * {@code jsm.getMessage(stream, seq)} — which is what the advisory bridge does on receipt — and
   * mirrors the production scenario where the bridge fires (handler hung past
   * {@code visibilityTimeout}, NATS still considers the message un-acked). The 2-minute
   * visibility timeout prevents NATS from redelivering before the test completes.
   */
  @Component
  static class BlockingListener {
    final CountDownLatch received = new CountDownLatch(1);

    @RqueueListener(value = "boom", visibilityTimeout = "120000")
    void onMessage(String payload) throws Exception {
      received.countDown();
      Thread.sleep(120_000L); // far exceeds the test's 30s budget
    }
  }
}
