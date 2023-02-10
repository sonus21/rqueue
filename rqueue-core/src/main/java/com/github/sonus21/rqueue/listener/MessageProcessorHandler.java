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

package com.github.sonus21.rqueue.listener;

import com.github.sonus21.rqueue.core.Job;
import com.github.sonus21.rqueue.core.support.MessageProcessor;
import com.github.sonus21.rqueue.models.enums.MessageStatus;
import com.github.sonus21.rqueue.utils.PrefixLogger;
import lombok.extern.slf4j.Slf4j;
import org.slf4j.event.Level;

@Slf4j
class MessageProcessorHandler extends PrefixLogger {

  private final MessageProcessor manualDeletionMessageProcessor;
  private final MessageProcessor deadLetterQueueMessageProcessor;
  private final MessageProcessor discardMessageProcessor;
  private final MessageProcessor postExecutionMessageProcessor;

  MessageProcessorHandler(
      MessageProcessor manualDeletionMessageProcessor,
      MessageProcessor deadLetterQueueMessageProcessor,
      MessageProcessor discardMessageProcessor,
      MessageProcessor postExecutionMessageProcessor) {
    super(log, null);
    this.manualDeletionMessageProcessor = manualDeletionMessageProcessor;
    this.deadLetterQueueMessageProcessor = deadLetterQueueMessageProcessor;
    this.discardMessageProcessor = discardMessageProcessor;
    this.postExecutionMessageProcessor = postExecutionMessageProcessor;
  }

  void handleMessage(Job job, MessageStatus status) {
    MessageProcessor messageProcessor = null;
    switch (status) {
      case DELETED:
        messageProcessor = manualDeletionMessageProcessor;
        break;
      case MOVED_TO_DLQ:
        messageProcessor = deadLetterQueueMessageProcessor;
        break;
      case DISCARDED:
        messageProcessor = discardMessageProcessor;
        break;
      case SUCCESSFUL:
        messageProcessor = postExecutionMessageProcessor;
        break;
      default:
        break;
    }
    if (messageProcessor != null) {
      try {
        log(Level.DEBUG, "Calling {} processor for {}", null, status, job.getRqueueMessage());
        messageProcessor.process(job);
      } catch (Exception e) {
        log(Level.ERROR, "Message processor {} call failed", e, status);
      }
    }
  }
}
