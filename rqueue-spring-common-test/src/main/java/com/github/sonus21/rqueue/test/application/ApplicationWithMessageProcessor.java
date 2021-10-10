/*
 *  Copyright 2021 Sonu Kumar
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *         https://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and limitations under the License.
 *
 */

package com.github.sonus21.rqueue.test.application;

import com.github.sonus21.rqueue.config.SimpleRqueueListenerContainerFactory;
import com.github.sonus21.rqueue.test.util.TestMessageProcessor;
import org.springframework.context.annotation.Bean;

public abstract class ApplicationWithMessageProcessor extends BaseApplication {

  @Bean
  public TestMessageProcessor deadLetterQueueMessageProcessor() {
    return new TestMessageProcessor();
  }

  @Bean
  public TestMessageProcessor preExecutionMessageProcessor() {
    return new TestMessageProcessor();
  }

  @Bean
  public TestMessageProcessor postExecutionMessageProcessor() {
    return new TestMessageProcessor();
  }

  @Bean
  public TestMessageProcessor manualDeletionMessageProcessor() {
    return new TestMessageProcessor();
  }

  @Bean
  public TestMessageProcessor discardMessageProcessor() {
    return new TestMessageProcessor();
  }

  @Bean
  public SimpleRqueueListenerContainerFactory simpleRqueueListenerContainerFactory() {
    SimpleRqueueListenerContainerFactory factory = new SimpleRqueueListenerContainerFactory();
    factory.setInspectAllBean(false);
    factory.setPreExecutionMessageProcessor(preExecutionMessageProcessor());
    factory.setPostExecutionMessageProcessor(postExecutionMessageProcessor());
    factory.setDiscardMessageProcessor(discardMessageProcessor());
    factory.setDeadLetterQueueMessageProcessor(deadLetterQueueMessageProcessor());
    factory.setManualDeletionMessageProcessor(manualDeletionMessageProcessor());
    return factory;
  }
}
