/*
 * Copyright (c) 2021-2023 Sonu Kumar
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

package com.github.sonus21.rqueue.test.application;

import com.github.sonus21.rqueue.config.SimpleRqueueListenerContainerFactory;
import com.github.sonus21.rqueue.test.util.TestMessageProcessor;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;

public abstract class ApplicationWithMessageProcessor extends BaseApplication {

  @Bean
  public TestMessageProcessor deadLetterQueueMessageProcessor() {
    return new TestMessageProcessor("DLQ");
  }

  @Bean
  public TestMessageProcessor preExecutionMessageProcessor() {
    return new TestMessageProcessor("PreEx");
  }

  @Bean
  public TestMessageProcessor postExecutionMessageProcessor() {
    return new TestMessageProcessor("PostEx");
  }

  @Bean
  public TestMessageProcessor manualDeletionMessageProcessor() {
    return new TestMessageProcessor("ManualDeletion");
  }

  @Bean
  public TestMessageProcessor discardMessageProcessor() {
    return new TestMessageProcessor("Discard");
  }

  @Bean
  public SimpleRqueueListenerContainerFactory simpleRqueueListenerContainerFactory(
      @Qualifier("preExecutionMessageProcessor") TestMessageProcessor preExecutionMessageProcessor,
      @Qualifier("postExecutionMessageProcessor") TestMessageProcessor postExecutionMessageProcessor,
      @Qualifier("manualDeletionMessageProcessor") TestMessageProcessor manualDeletionMessageProcessor,
      @Qualifier("discardMessageProcessor") TestMessageProcessor discardMessageProcessor,
      @Qualifier("deadLetterQueueMessageProcessor") TestMessageProcessor deadLetterQueueMessageProcessor) {
    SimpleRqueueListenerContainerFactory factory = new SimpleRqueueListenerContainerFactory();
    factory.setInspectAllBean(false);
    factory.setPreExecutionMessageProcessor(preExecutionMessageProcessor);
    factory.setPostExecutionMessageProcessor(postExecutionMessageProcessor);
    factory.setDiscardMessageProcessor(discardMessageProcessor);
    factory.setDeadLetterQueueMessageProcessor(deadLetterQueueMessageProcessor);
    factory.setManualDeletionMessageProcessor(manualDeletionMessageProcessor);
    return factory;
  }
}
