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
import com.github.sonus21.rqueue.test.dto.Email;
import com.github.sonus21.rqueue.test.dto.FeedGeneration;
import com.github.sonus21.rqueue.test.dto.Job;
import com.github.sonus21.rqueue.test.dto.Reservation;
import com.github.sonus21.rqueue.test.dto.Sms;
import com.github.sonus21.rqueue.utils.Constants;
import com.github.sonus21.rqueue.utils.TimeoutUtils;
import java.util.Arrays;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public abstract class AllQueueMode extends SpringTestBase {

  protected void checkGroupConsumer() throws TimedOutException {
    enqueue(chatIndexingQueue, ChatIndexing.newInstance());
    enqueue(feedGenerationQueue, FeedGeneration.newInstance());
    enqueue(reservationQueue, Reservation.newInstance());
    TimeoutUtils.waitFor(
        () ->
            getMessageCount(Arrays.asList(chatIndexingQueue, feedGenerationQueue, reservationQueue))
                == 0,
        20 * Constants.ONE_MILLI,
        "group queues to drain");
  }

  protected void checkQueueLevelConsumer() throws TimedOutException {
    enqueue(smsQueue, Sms.newInstance());
    enqueueWithPriority(smsQueue, "critical", Sms.newInstance());
    enqueueWithPriority(smsQueue, "high", Sms.newInstance());
    enqueueWithPriority(smsQueue, "medium", Sms.newInstance());
    enqueueWithPriority(smsQueue, "low", Sms.newInstance());
    TimeoutUtils.waitFor(
        () ->
            getMessageCount(
                Arrays.asList(
                    smsQueue,
                    smsQueue + "_critical",
                    smsQueue + "_high",
                    smsQueue + "_medium",
                    smsQueue + "_low"))
                == 0,
        20 * Constants.ONE_MILLI,
        "multi level queues to drain");
  }

  protected void testSimpleConsumer() throws TimedOutException {
    enqueue(emailQueue, Email.newInstance());
    enqueueIn(emailQueue, Email.newInstance(), 1000L);
    enqueue(jobQueue, Job.newInstance());
    enqueueIn(jobQueue, Job.newInstance(), 2000L, TimeUnit.MILLISECONDS);

    TimeoutUtils.waitFor(
        () -> getMessageCount(Arrays.asList(emailQueue, jobQueue)) == 0,
        20 * Constants.ONE_MILLI,
        "simple queues to drain");
  }
}
