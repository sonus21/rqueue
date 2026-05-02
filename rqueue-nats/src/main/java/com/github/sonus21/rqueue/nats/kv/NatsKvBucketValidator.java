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

package com.github.sonus21.rqueue.nats.kv;

import io.nats.client.Connection;
import io.nats.client.JetStreamApiException;
import io.nats.client.KeyValueManagement;
import io.nats.client.api.KeyValueStatus;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.springframework.beans.factory.InitializingBean;

/**
 * Startup-time guard for deployments where the application credentials cannot create JetStream
 * KV buckets at runtime.
 *
 * <p><b>Where this runs.</b> The Spring Boot auto-config calls {@link #validate(Connection,
 * boolean)} <i>inline</i> in the {@code natsConnection} bean factory method, so validation
 * happens during {@code Connection} bean creation — strictly before any other NATS-coupled
 * bean (broker, daos, registry, lock manager) can be wired with the connection. Spring's
 * {@code @Order}/{@code @Priority} only affect collection injection ordering, not bean
 * creation order, so we anchor the check on the dependency root instead.
 *
 * <p><b>Bean form.</b> The auto-config also declares this class as a Spring bean named
 * {@code natsKvBucketValidator} so that every NATS-coupled bean can {@code @DependsOn} it.
 * The bean implements {@link InitializingBean} and re-runs the same validation on its
 * {@code afterPropertiesSet}; this is a cheap belt-and-suspenders check (when buckets already
 * exist, all calls are {@code getStatus} no-ops). The class itself is config-source-agnostic
 * — it takes the {@code autoCreate} flag as a constructor argument, so reading
 * {@code rqueue.nats.autoCreateKvBuckets} from any property source is the responsibility of
 * the caller (in Spring Boot, that's {@code RqueueNatsProperties}).
 *
 * <p>When {@code rqueue.nats.autoCreateKvBuckets=true} (default) the validator is a no-op:
 * the existing lazy-create paths in each store / dao remain in charge.
 *
 * <p>When {@code rqueue.nats.autoCreateKvBuckets=false} the validator walks
 * {@link NatsKvBuckets#ALL_BUCKETS} via {@link KeyValueManagement#getStatus(String)} and aborts
 * boot with an {@link IllegalStateException} listing every missing bucket. This converts a
 * subtle, late-binding "permission violation on first use" failure into a deterministic
 * startup failure with operator-facing remediation (the README NATS section lists the
 * {@code nats kv add} commands).
 *
 * <p>The validator does NOT pre-create buckets on its own — auto-creation, when enabled, stays
 * lazy because some buckets (jobs, locks) only learn their TTL on first write. If you need
 * eager creation with knowable TTLs, follow up by extending this class.
 */
public class NatsKvBucketValidator implements InitializingBean {

  private static final Logger log = Logger.getLogger(NatsKvBucketValidator.class.getName());

  private final Connection connection;
  private final boolean autoCreate;

  public NatsKvBucketValidator(Connection connection, boolean autoCreate) {
    this.connection = connection;
    this.autoCreate = autoCreate;
  }

  @Override
  public void afterPropertiesSet() {
    validate(connection, autoCreate);
  }

  /**
   * Canonical entry point. Call this before exposing the {@link Connection} to any other
   * Rqueue bean: the Spring Boot auto-config does so inside the {@code natsConnection} bean
   * factory method so the connection is already validated by the time the broker, daos,
   * registry, and lock manager beans inject it.
   *
   * @throws IllegalStateException if {@code autoCreate} is {@code false} and any required KV
   *     bucket is missing, or if the JetStream KV management API is unreachable.
   */
  public static void validate(Connection connection, boolean autoCreate) {
    if (autoCreate) {
      log.fine("rqueue.nats.autoCreateKvBuckets=true; skipping startup KV bucket validation, stores"
          + " will lazily create buckets as needed.");
      return;
    }
    KeyValueManagement kvm;
    try {
      kvm = connection.keyValueManagement();
    } catch (IOException io) {
      throw new IllegalStateException(
          "Failed to obtain JetStream KeyValueManagement from NATS connection while validating"
              + " KV buckets. Check that JetStream is enabled and reachable.",
          io);
    }
    List<String> missing = new ArrayList<>();
    for (String bucket : NatsKvBuckets.ALL_BUCKETS) {
      if (!exists(kvm, bucket)) {
        missing.add(bucket);
      }
    }
    if (missing.isEmpty()) {
      log.info("All required NATS KV buckets are present: " + NatsKvBuckets.ALL_BUCKETS);
      return;
    }
    throw new IllegalStateException(
        "rqueue.nats.autoCreateKvBuckets=false but the following NATS KV buckets are missing: "
            + String.join(", ", missing)
            + ". Pre-create them via `nats kv add ...` (see the 'NATS backend' section of the"
            + " README for ready-made commands) and restart, or set"
            + " rqueue.nats.autoCreateKvBuckets=true to allow Rqueue to create them at runtime.");
  }

  private static boolean exists(KeyValueManagement kvm, String bucket) {
    try {
      KeyValueStatus status = kvm.getStatus(bucket);
      return status != null;
    } catch (JetStreamApiException missing) {
      return false;
    } catch (IOException io) {
      // Surface IO problems separately — the operator can't fix a missing bucket if the
      // connection itself is broken.
      log.log(Level.WARNING, "KV status check for bucket " + bucket + " failed", io);
      return false;
    }
  }
}
