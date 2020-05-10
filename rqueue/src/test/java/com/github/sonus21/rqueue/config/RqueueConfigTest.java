/*
 * Copyright 2020 Sonu Kumar
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.github.sonus21.rqueue.config;

import static org.junit.Assert.assertEquals;

import org.junit.Before;
import org.junit.Test;

public class RqueueConfigTest {
  private RqueueConfig rqueueConfigVersion1 = new RqueueConfig(null, false, 1);
  private RqueueConfig rqueueConfigVersion2 = new RqueueConfig(null, false, 2);
  private RqueueConfig rqueueConfigVersion2ClusterMode = new RqueueConfig(null, false, 2);

  private void initialize(RqueueConfig rqueueConfig) {
    rqueueConfig.setPrefix("__rq::");
    rqueueConfig.setSimpleQueuePrefix("queue::");
    rqueueConfig.setDelayedQueuePrefix("d-queue::");
    rqueueConfig.setDelayedQueueChannelPrefix("d-channel::");
    rqueueConfig.setProcessingQueuePrefix("p-queue::");
    rqueueConfig.setProcessingQueueChannelPrefix("p-channel::");
    rqueueConfig.setLockKeyPrefix("lock::");
    rqueueConfig.setQueueConfigKeyPrefix("q-config::");
    rqueueConfig.setQueueStatKeyPrefix("q-stat::");
  }

  @Before
  public void init() {
    initialize(rqueueConfigVersion1);
    initialize(rqueueConfigVersion2);
    initialize(rqueueConfigVersion2ClusterMode);
    rqueueConfigVersion2ClusterMode.setClusterMode(true);
  }

  @Test
  public void getQueueName() {
    assertEquals("test", rqueueConfigVersion1.getQueueName("test"));
    assertEquals("__rq::queue::test", rqueueConfigVersion2.getQueueName("test"));
    assertEquals("__rq::queue::{test}", rqueueConfigVersion2ClusterMode.getQueueName("test"));
  }

  @Test
  public void getDelayedQueueName() {
    assertEquals("rqueue-delay::test", rqueueConfigVersion1.getDelayedQueueName("test"));
    assertEquals("__rq::d-queue::test", rqueueConfigVersion2.getDelayedQueueName("test"));
    assertEquals(
        "__rq::d-queue::{test}", rqueueConfigVersion2ClusterMode.getDelayedQueueName("test"));
  }

  @Test
  public void getDelayedQueueChannelName() {
    assertEquals("rqueue-channel::test", rqueueConfigVersion1.getDelayedQueueChannelName("test"));
    assertEquals("__rq::d-channel::test", rqueueConfigVersion2.getDelayedQueueChannelName("test"));
    assertEquals("__rq::d-channel::{test}", rqueueConfigVersion2ClusterMode.getDelayedQueueChannelName("test"));
  }

  @Test
  public void getProcessingQueueName() {
    assertEquals("rqueue-processing::test", rqueueConfigVersion1.getProcessingQueueName("test"));
    assertEquals("__rq::p-queue::test", rqueueConfigVersion2.getProcessingQueueName("test"));
    assertEquals(
        "__rq::p-queue::{test}", rqueueConfigVersion2ClusterMode.getProcessingQueueName("test"));
  }

  @Test
  public void getProcessingQueueChannelName() {
    assertEquals(
        "rqueue-processing-channel::test",
        rqueueConfigVersion1.getProcessingQueueChannelName("test"));
    assertEquals(
        "__rq::p-channel::test", rqueueConfigVersion2.getProcessingQueueChannelName("test"));
    assertEquals(
        "__rq::p-channel::{test}",
        rqueueConfigVersion2ClusterMode.getProcessingQueueChannelName("test"));
  }

  @Test
  public void getLockKey() {
    assertEquals("__rq::lock::test", rqueueConfigVersion1.getLockKey("test"));
    assertEquals("__rq::lock::test", rqueueConfigVersion2.getLockKey("test"));
    assertEquals("__rq::lock::test", rqueueConfigVersion2ClusterMode.getLockKey("test"));
  }

  @Test
  public void getQueueStatisticsKey() {
    assertEquals("__rq::q-stat::test", rqueueConfigVersion1.getQueueStatisticsKey("test"));
    assertEquals("__rq::q-stat::test", rqueueConfigVersion2.getQueueStatisticsKey("test"));
    assertEquals(
        "__rq::q-stat::test", rqueueConfigVersion2ClusterMode.getQueueStatisticsKey("test"));
  }

  @Test
  public void getQueueConfigKey() {
    assertEquals("__rq::q-config::test", rqueueConfigVersion1.getQueueConfigKey("test"));
    assertEquals("__rq::q-config::test", rqueueConfigVersion2.getQueueConfigKey("test"));
    assertEquals("__rq::q-config::test", rqueueConfigVersion2ClusterMode.getQueueConfigKey("test"));
  }
}
