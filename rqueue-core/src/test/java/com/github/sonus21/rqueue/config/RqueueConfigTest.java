/*
 * Copyright (c) 2020-2023 Sonu Kumar
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

package com.github.sonus21.rqueue.config;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.github.sonus21.TestBase;
import com.github.sonus21.rqueue.CoreUnitTest;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

@CoreUnitTest
class RqueueConfigTest extends TestBase {

  private final RqueueConfig rqueueConfigVersion1 = new RqueueConfig(null, null, false, 1);
  private final RqueueConfig rqueueConfigVersion2 = new RqueueConfig(null, null, false, 2);
  private final RqueueConfig rqueueConfigVersion2ClusterMode =
      new RqueueConfig(null, null, false, 2);

  private void initialize(RqueueConfig rqueueConfig) {
    rqueueConfig.setPrefix("__rq::");
    rqueueConfig.setSimpleQueuePrefix("queue::");
    rqueueConfig.setScheduledQueuePrefix("d-queue::");
    rqueueConfig.setScheduledQueueChannelPrefix("d-channel::");
    rqueueConfig.setProcessingQueuePrefix("p-queue::");
    rqueueConfig.setProcessingQueueChannelPrefix("p-channel::");
    rqueueConfig.setLockKeyPrefix("lock::");
    rqueueConfig.setQueueConfigKeyPrefix("q-config::");
    rqueueConfig.setQueueStatKeyPrefix("q-stat::");
  }

  @BeforeEach
  public void init() {
    initialize(rqueueConfigVersion1);
    initialize(rqueueConfigVersion2);
    initialize(rqueueConfigVersion2ClusterMode);
    rqueueConfigVersion2ClusterMode.setClusterMode(true);
  }

  @Test
  void getQueueName() {
    assertEquals("test", rqueueConfigVersion1.getQueueName("test"));
    assertEquals("__rq::queue::test", rqueueConfigVersion2.getQueueName("test"));
    assertEquals("__rq::queue::{test}", rqueueConfigVersion2ClusterMode.getQueueName("test"));
  }

  @Test
  void getScheduledQueueName() {
    assertEquals("rqueue-delay::test", rqueueConfigVersion1.getScheduledQueueName("test"));
    assertEquals("__rq::d-queue::test", rqueueConfigVersion2.getScheduledQueueName("test"));
    assertEquals(
        "__rq::d-queue::{test}", rqueueConfigVersion2ClusterMode.getScheduledQueueName("test"));
  }

  @Test
  void getScheduledQueueChannelName() {
    assertEquals("rqueue-channel::test", rqueueConfigVersion1.getScheduledQueueChannelName("test"));
    assertEquals(
        "__rq::d-channel::test", rqueueConfigVersion2.getScheduledQueueChannelName("test"));
    assertEquals(
        "__rq::d-channel::{test}",
        rqueueConfigVersion2ClusterMode.getScheduledQueueChannelName("test"));
  }

  @Test
  void getProcessingQueueName() {
    assertEquals("rqueue-processing::test", rqueueConfigVersion1.getProcessingQueueName("test"));
    assertEquals("__rq::p-queue::test", rqueueConfigVersion2.getProcessingQueueName("test"));
    assertEquals(
        "__rq::p-queue::{test}", rqueueConfigVersion2ClusterMode.getProcessingQueueName("test"));
  }

  @Test
  void getProcessingQueueChannelName() {
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
  void getLockKey() {
    assertEquals("__rq::lock::test", rqueueConfigVersion1.getLockKey("test"));
    assertEquals("__rq::lock::test", rqueueConfigVersion2.getLockKey("test"));
    assertEquals("__rq::lock::test", rqueueConfigVersion2ClusterMode.getLockKey("test"));
  }

  @Test
  void getQueueStatisticsKey() {
    assertEquals("__rq::q-stat::test", rqueueConfigVersion1.getQueueStatisticsKey("test"));
    assertEquals("__rq::q-stat::test", rqueueConfigVersion2.getQueueStatisticsKey("test"));
    assertEquals(
        "__rq::q-stat::test", rqueueConfigVersion2ClusterMode.getQueueStatisticsKey("test"));
  }

  @Test
  void getQueueConfigKey() {
    assertEquals("__rq::q-config::test", rqueueConfigVersion1.getQueueConfigKey("test"));
    assertEquals("__rq::q-config::test", rqueueConfigVersion2.getQueueConfigKey("test"));
    assertEquals("__rq::q-config::test", rqueueConfigVersion2ClusterMode.getQueueConfigKey("test"));
  }
}
