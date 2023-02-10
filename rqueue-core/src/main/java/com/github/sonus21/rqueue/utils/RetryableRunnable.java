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

package com.github.sonus21.rqueue.utils;

import java.util.Iterator;
import org.apache.commons.lang3.NotImplementedException;
import org.slf4j.Logger;
import org.slf4j.event.Level;

public abstract class RetryableRunnable<V> extends PrefixLogger implements Runnable {

  private Iterator<V> generator;

  protected RetryableRunnable(Logger log, String groupName) {
    super(log, groupName);
  }

  protected RetryableRunnable(Logger log, String groupName, Iterator<V> generator) {
    super(log, groupName);
    this.generator = generator;
  }

  public void consume(V v) {
    throw new NotImplementedException("method is not implemented");
  }

  public void start() {
    throw new NotImplementedException("method is not implemented");
  }

  @Override
  public void run() {
    if (generator == null) {
      try {
        start();
      } catch (Exception e) {
        log(Level.ERROR, "Failed {}", e, e.getMessage());
      }
    } else {
      while (generator.hasNext()) {
        V v = generator.next();
        try {
          consume(v);
        } catch (Exception e) {
          log(Level.ERROR, "Failed {} V: {}", e, e.getMessage(), v);
        }
      }
    }
  }
}
