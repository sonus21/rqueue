/*
 * Copyright (c) 2024-2026 Sonu Kumar
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * You may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 */
package com.github.sonus21.rqueue.nats;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.github.sonus21.rqueue.config.RqueueConfig;
import com.github.sonus21.rqueue.core.EndpointRegistry;
import com.github.sonus21.rqueue.enums.QueueType;
import com.github.sonus21.rqueue.listener.QueueDetail;
import com.github.sonus21.rqueue.models.enums.RqueueMode;
import com.github.sonus21.rqueue.nats.internal.NatsProvisioner;
import com.github.sonus21.rqueue.nats.js.NatsStreamValidator;
import java.time.Duration;
import java.util.Collections;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Unit-tests the producer-mode skip in {@link NatsStreamValidator}: streams must still be
 * provisioned (publishers need them) but the per-queue durable consumer must NOT be created when
 * the application is configured as a producer-only node.
 */
@NatsUnitTest
class NatsStreamValidatorProducerModeTest {

  private NatsProvisioner provisioner;
  private RqueueNatsConfig natsConfig;

  private static QueueDetail queue(String name) {
    return QueueDetail.builder()
        .name(name)
        .queueName(name)
        .processingQueueName(name + "-pq")
        .completedQueueName(name + "-cq")
        .scheduledQueueName(name + "-sq")
        .processingQueueChannelName(name + "-pch")
        .scheduledQueueChannelName(name + "-sch")
        .visibilityTimeout(30000)
        .numRetry(3)
        .priority(Collections.emptyMap())
        .active(true)
        .type(QueueType.QUEUE)
        .build();
  }

  @BeforeEach
  void setUp() {
    EndpointRegistry.delete();
    provisioner = mock(NatsProvisioner.class);
    when(provisioner.ensureConsumer(
            anyString(), anyString(), anyString(), any(), anyLong(), anyLong()))
        .thenReturn("rqueue-consumer");
    natsConfig = RqueueNatsConfig.defaults();
  }

  @Test
  void producerMode_skipsConsumerProvisioningButStillEnsuresStream() {
    EndpointRegistry.register(queue("orders"));
    RqueueConfig rqueueConfig = mock(RqueueConfig.class);
    when(rqueueConfig.getMode()).thenReturn(RqueueMode.PRODUCER);
    when(rqueueConfig.isProducer()).thenReturn(true);

    NatsStreamValidator validator = new NatsStreamValidator(provisioner, natsConfig, rqueueConfig);
    validator.afterSingletonsInstantiated();

    verify(provisioner, times(1))
        .ensureStream(eq(natsConfig.getStreamPrefix() + "orders"), any(), any(), any());
    verify(provisioner, never())
        .ensureConsumer(
            anyString(), anyString(), anyString(), any(Duration.class), anyLong(), anyLong());
  }

  @Test
  void consumerMode_provisionsBothStreamAndConsumer() {
    EndpointRegistry.register(queue("orders"));
    RqueueConfig rqueueConfig = mock(RqueueConfig.class);
    when(rqueueConfig.getMode()).thenReturn(RqueueMode.BOTH);

    NatsStreamValidator validator = new NatsStreamValidator(provisioner, natsConfig, rqueueConfig);
    validator.afterSingletonsInstantiated();

    verify(provisioner, times(1))
        .ensureStream(eq(natsConfig.getStreamPrefix() + "orders"), any(), any(), any());
    verify(provisioner, times(1))
        .ensureConsumer(
            eq(natsConfig.getStreamPrefix() + "orders"),
            anyString(),
            eq(natsConfig.getSubjectPrefix() + "orders"),
            any(Duration.class),
            anyLong(),
            anyLong());
  }

  @Test
  void nullRqueueConfig_treatedAsConsumerMode_provisionsBoth() {
    EndpointRegistry.register(queue("orders"));

    NatsStreamValidator validator = new NatsStreamValidator(provisioner, natsConfig);
    validator.afterSingletonsInstantiated();

    verify(provisioner, times(1))
        .ensureConsumer(
            anyString(), anyString(), anyString(), any(Duration.class), anyLong(), anyLong());
  }
}
