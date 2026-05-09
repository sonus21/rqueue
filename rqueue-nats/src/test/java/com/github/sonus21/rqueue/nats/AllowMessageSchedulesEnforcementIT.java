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

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import io.nats.client.JetStream;
import io.nats.client.JetStreamManagement;
import io.nats.client.api.RetentionPolicy;
import io.nats.client.api.StorageType;
import io.nats.client.api.StreamConfiguration;
import io.nats.client.api.StreamInfo;
import io.nats.client.impl.Headers;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import org.junit.jupiter.api.Test;

/**
 * Investigates whether NATS server actually enforces the {@code allow_msg_schedules} stream flag
 * when a message is published with the {@code Nats-Schedule} header (ADR-51, NATS >= 2.12).
 *
 * <p>The unit tests confirm that {@link com.github.sonus21.rqueue.nats.internal.NatsProvisioner}
 * sets the flag correctly, but they mock {@code jsm.addStream()} — they cannot prove the server
 * enforces it. This IT creates a stream <em>without</em> the flag against a real
 * {@code nats:2.12-alpine} container (or an externally-managed server) and attempts the publish
 * directly, recording whether the server accepts or rejects it.
 *
 * <p><b>Expected result:</b> NATS 2.12+ enforces the flag; the publish should be rejected (throw)
 * with error 10189. If the publish succeeds the test documents that the flag is advisory on this
 * server version, so that the team knows the protective unit tests are not backed by enforcement.
 */
@NatsIntegrationTest
class AllowMessageSchedulesEnforcementIT extends AbstractJetStreamIT {

  private static final DateTimeFormatter RFC3339 =
      DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss'Z'").withZone(ZoneOffset.UTC);

  @Test
  void publishWithScheduleHeader_streamWithoutFlag_serverRejectsIt() throws Exception {
    JetStreamManagement jsm = connection.jetStreamManagement();
    JetStream js = connection.jetStream();

    // Only meaningful on servers that support scheduling at all
    assumeTrue(
        connection.getServerInfo().isSameOrNewerThanVersion("2.12.0"),
        "Skipping: server < 2.12 — Nats-Schedule header not supported");

    String nano = String.valueOf(System.nanoTime());
    String streamName = "enforce-sched-test-" + nano;
    // Use a sched-subject pattern so the subject is valid for scheduling publish
    String workSubject = "rqueue.enforce." + nano;
    String schedSubject = workSubject + ".sched.probe";

    // Create stream deliberately WITHOUT allowMessageSchedules, but including both subjects
    StreamConfiguration cfg = StreamConfiguration.builder()
        .name(streamName)
        .subjects(workSubject, schedSubject)
        .storageType(StorageType.Memory)
        .retentionPolicy(RetentionPolicy.WorkQueue)
        .allowMessageSchedules(false)
        .build();
    jsm.addStream(cfg);

    // Confirm the flag is not set on the server-side config
    StreamInfo info = jsm.getStreamInfo(streamName);
    assertFalse(info.getConfiguration().getAllowMsgSchedules(),
        "Pre-condition: stream must NOT have allow_msg_schedules");

    // Build correct ADR-51 headers: Nats-Schedule + Nats-Schedule-Target + Nats-Rollup
    String deliverAt = "@at " + RFC3339.format(Instant.now().plusSeconds(10));
    Headers headers = new Headers();
    headers.add("Nats-Schedule", deliverAt);
    headers.add("Nats-Schedule-Target", workSubject);
    headers.add("Nats-Rollup", "sub");

    boolean serverEnforcesFlag;
    try {
      js.publish(schedSubject, headers, "probe".getBytes());
      // Publish succeeded — server does NOT enforce the flag
      serverEnforcesFlag = false;
    } catch (Exception e) {
      // Publish rejected (expected: error 10189) — server DOES enforce the flag
      serverEnforcesFlag = true;
    }

    // Clean up
    jsm.deleteStream(streamName);

    // Document and assert the actual behaviour.
    // If this assertion ever changes it means a NATS server upgrade changed the enforcement policy.
    assertTrue(serverEnforcesFlag,
        "NATS server accepted Nats-Schedule on a stream without allow_msg_schedules. "
            + "This means the flag is advisory on this server version — the unit tests in "
            + "NatsProvisionerTest that verify the flag is set are still correct best-practice "
            + "but the server does not enforce it. Update this test if the behaviour is intentional.");
  }

  @Test
  void publishWithScheduleHeader_streamWithFlag_serverAcceptsIt() throws Exception {
    JetStreamManagement jsm = connection.jetStreamManagement();
    JetStream js = connection.jetStream();

    assumeTrue(
        connection.getServerInfo().isSameOrNewerThanVersion("2.12.0"),
        "Skipping: server < 2.12 — Nats-Schedule header not supported");

    String nano = String.valueOf(System.nanoTime());
    String streamName = "enforce-sched-ok-" + nano;
    String workSubject = "rqueue.enforce.ok." + nano;
    String schedSubject = workSubject + ".sched.probe";

    // Create stream WITH allowMessageSchedules
    StreamConfiguration cfg = StreamConfiguration.builder()
        .name(streamName)
        .subjects(workSubject, schedSubject)
        .storageType(StorageType.Memory)
        .retentionPolicy(RetentionPolicy.WorkQueue)
        .allowMessageSchedules(true)
        .build();
    jsm.addStream(cfg);

    StreamInfo info = jsm.getStreamInfo(streamName);
    assertTrue(info.getConfiguration().getAllowMsgSchedules(),
        "Pre-condition: stream MUST have allow_msg_schedules");

    String deliverAt = "@at " + RFC3339.format(Instant.now().plusSeconds(10));
    Headers headers = new Headers();
    headers.add("Nats-Schedule", deliverAt);
    headers.add("Nats-Schedule-Target", workSubject);
    headers.add("Nats-Rollup", "sub");

    // With the flag set this must never throw
    assertDoesNotThrow(
        () -> js.publish(schedSubject, headers, "probe".getBytes()),
        "Publish with Nats-Schedule must succeed when allow_msg_schedules=true");

    assertNotNull(jsm.deleteStream(streamName));
  }
}
