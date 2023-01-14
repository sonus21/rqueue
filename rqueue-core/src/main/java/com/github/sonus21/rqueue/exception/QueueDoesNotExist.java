/*
 * Copyright (c) 2019-2023 Sonu Kumar
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

package com.github.sonus21.rqueue.exception;

/**
 * This exception is raised, when a queue is not registered in
 * {@link com.github.sonus21.rqueue.core.EndpointRegistry}, to register a queue use
 * {@link com.github.sonus21.rqueue.core.RqueueEndpointManager#registerQueue(String, String...)}
 */
public class QueueDoesNotExist extends RuntimeException {

  private static final long serialVersionUID = 598739372785907190L;

  public QueueDoesNotExist(String name) {
    super(name);
  }
}
