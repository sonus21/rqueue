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

package com.github.sonus21.rqueue.test.tests;

import com.github.sonus21.rqueue.exception.TimedOutException;
import com.github.sonus21.rqueue.test.common.SpringTestBase;
import com.github.sonus21.rqueue.test.dto.ChatIndexing;
import com.github.sonus21.rqueue.test.dto.FeedGeneration;
import com.github.sonus21.rqueue.test.dto.Reservation;
import com.github.sonus21.rqueue.utils.TimeoutUtils;
import java.util.Arrays;

public abstract class GroupPriorityTest extends SpringTestBase {

  protected void checkGroupConsumer() throws TimedOutException {
    enqueue(chatIndexingQueue, ChatIndexing.newInstance());
    enqueue(feedGenerationQueue, FeedGeneration.newInstance());
    enqueue(reservationQueue, Reservation.newInstance());
    TimeoutUtils.waitFor(
        () ->
            getMessageCount(Arrays.asList(chatIndexingQueue, feedGenerationQueue, reservationQueue))
                == 0,
        "Waiting for group queues to drain");
  }
}
