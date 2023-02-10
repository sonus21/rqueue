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

public class ProcessingException extends Exception {

  private static final long serialVersionUID = 9003794852942108696L;
  private final Object data;

  public ProcessingException(String message) {
    this(message, null);
  }

  public ProcessingException(String message, Object data) {
    super(message);
    this.data = null;
  }

  @Override
  public String toString() {
    if (data == null) {
      return super.toString();
    }
    return super.toString() + data;
  }
}
