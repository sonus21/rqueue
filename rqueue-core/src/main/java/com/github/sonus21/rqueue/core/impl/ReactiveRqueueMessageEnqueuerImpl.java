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

package com.github.sonus21.rqueue.core.impl;

import com.github.sonus21.rqueue.core.ReactiveRqueueMessageEnqueuer;
import reactor.core.publisher.Mono;

public class ReactiveRqueueMessageEnqueuerImpl implements ReactiveRqueueMessageEnqueuer {

  @Override
  public Mono<String> enqueue(String queueName, Object message) {
    return null;
  }

  @Override
  public Mono<Boolean> enqueue(String queueName, String messageId, Object message) {
    return null;
  }

  @Override
  public Mono<Boolean> enqueueUnique(String queueName, String messageId, Object message) {
    return null;
  }

  @Override
  public Mono<String> enqueueWithRetry(String queueName, Object message, int retryCount) {
    return null;
  }

  @Override
  public Mono<Boolean> enqueueWithRetry(String queueName, String messageId, Object message,
      int retryCount) {
    return null;
  }

  @Override
  public Mono<String> enqueueWithPriority(String queueName, String priority, Object message) {
    return null;
  }

  @Override
  public Mono<Boolean> enqueueWithPriority(String queueName, String priority,
      String messageId, Object message) {
    return null;
  }

  @Override
  public Mono<String> enqueueIn(String queueName, Object message, long delayInMilliSecs) {
    return null;
  }

  @Override
  public Mono<Boolean> enqueueIn(String queueName, String messageId, Object message,
      long delayInMilliSecs) {
    return null;
  }

  @Override
  public Mono<Boolean> enqueueUniqueIn(String queueName, String messageId, Object message,
      long delayInMillisecond) {
    return null;
  }

  @Override
  public Mono<String> enqueueInWithRetry(String queueName, Object message, int retryCount,
      long delayInMilliSecs) {
    return null;
  }

  @Override
  public Mono<Boolean> enqueueInWithRetry(String queueName, String messageId,
      Object message, int retryCount, long delayInMilliSecs) {
    return null;
  }

  @Override
  public Mono<String> enqueuePeriodic(String queueName, Object message, long periodInMilliSeconds) {
    return null;
  }

  @Override
  public Mono<Boolean> enqueuePeriodic(String queueName, String messageId, Object message,
      long periodInMilliSeconds) {
    return null;
  }
}
