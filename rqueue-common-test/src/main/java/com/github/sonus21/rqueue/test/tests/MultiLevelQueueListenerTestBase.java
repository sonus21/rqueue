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

package com.github.sonus21.rqueue.test.tests;

import com.github.sonus21.rqueue.exception.TimedOutException;
import com.github.sonus21.rqueue.test.dto.Otp;
import com.github.sonus21.rqueue.utils.TimeoutUtils;
import java.util.Arrays;

public abstract class MultiLevelQueueListenerTestBase extends SpringTestBase {
  protected void checkQueueLevelConsumer() throws TimedOutException {
    rqueueMessageSender.enqueue(otpQueue, Otp.newInstance());
    rqueueMessageSender.enqueueWithPriority(otpQueue, "critical", Otp.newInstance());
    rqueueMessageSender.enqueueWithPriority(otpQueue, "high", Otp.newInstance());
    rqueueMessageSender.enqueueWithPriority(otpQueue, "medium", Otp.newInstance());
    rqueueMessageSender.enqueueWithPriority(otpQueue, "low", Otp.newInstance());
    TimeoutUtils.waitFor(
        () ->
            getMessageCount(
                    Arrays.asList(
                        otpQueue,
                        otpQueue + "_critical",
                        otpQueue + "_high",
                        otpQueue + "_medium",
                        otpQueue + "_low"))
                == 0,
        "Waiting for multi level queues to drain");
  }
}
