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

package com.github.sonus21.rqueue.listener;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

public class QueueDetailTest {

  @Test(expected = IllegalArgumentException.class)
  public void constructWithNegativeRetryCount() {
    new QueueDetail("test", -1, null, false, 1000L);
  }

  @Test(expected = NullPointerException.class)
  public void constructWithNullQueueName() {
    new QueueDetail(null, 100, null, false, 1000L);
  }

  @Test
  public void isDlqSet() {
    QueueDetail queueDetail = new QueueDetail("test", 100, null, false, 1000L);
    assertFalse(queueDetail.isDlqSet());
    queueDetail = new QueueDetail("test", 100, "", false, 1000L);
    assertFalse(queueDetail.isDlqSet());

    queueDetail = new QueueDetail("test", 100, "dlq-test", false, 1000L);
    assertTrue(queueDetail.isDlqSet());
  }
}
