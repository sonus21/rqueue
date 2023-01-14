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
 * Whenever a lock can not be acquired cause some other process/thread is holding lock then this
 * error would be thrown. The application should retry once this error occurs.
 */
public class LockCanNotBeAcquired extends RuntimeException {

  private static final long serialVersionUID = 598739372785907190L;

  public LockCanNotBeAcquired(String name) {
    super(name);
  }
}
