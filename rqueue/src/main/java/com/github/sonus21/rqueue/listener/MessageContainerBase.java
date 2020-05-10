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

package com.github.sonus21.rqueue.listener;

import com.github.sonus21.rqueue.core.RqueueMessageTemplate;
import java.lang.ref.WeakReference;
import java.util.Objects;
import org.slf4j.Logger;
import org.slf4j.event.Level;

abstract class MessageContainerBase implements Runnable {
  protected final WeakReference<RqueueMessageListenerContainer> container;
  private final Logger log;
  private final String groupName;

  MessageContainerBase(Logger log, String groupName, RqueueMessageListenerContainer container) {
    this(log, groupName, new WeakReference<>(container));
  }

  MessageContainerBase(
      Logger log, String groupName, WeakReference<RqueueMessageListenerContainer> container) {
    this.log = log;
    this.groupName = groupName;
    this.container = container;
  }

  protected RqueueMessageTemplate getRqueueMessageTemplate() {
    return Objects.requireNonNull(container.get()).getRqueueMessageTemplate();
  }

  boolean isQueueActive(String queueName) {
    return Objects.requireNonNull(container.get()).isQueueActive(queueName);
  }

  void log(Level level, String msg, Throwable t, Object... objects) {
    if (level == Level.DEBUG && !log.isDebugEnabled()) {
      return;
    }
    if (level == Level.ERROR && !log.isErrorEnabled()) {
      return;
    }
    if (level == Level.INFO && !log.isInfoEnabled()) {
      return;
    }
    if (level == Level.WARN && !log.isWarnEnabled()) {
      return;
    }
    Object[] objects1 = new Object[objects.length + 1 + (t == null ? 0 : 1)];
    System.arraycopy(objects, 0, objects1, 1, objects.length);
    objects1[0] = groupName;
    if (t != null) {
      objects1[objects1.length - 1] = t;
    }
    String txt = "[{}] " + msg;
    switch (level) {
      case INFO:
        log.info(txt, objects1);
        break;
      case WARN:
        log.warn(txt, objects1);
        break;
      case DEBUG:
        log.debug(txt, objects1);
        break;
      case ERROR:
        log.error(txt, objects1);
        break;
      default:
        log.trace(txt, objects1);
    }
  }

  @Override
  public void run() {
    try {
      start();
    } catch (Exception e) {
      log(Level.ERROR, "Failed {}", e, e.getMessage());
    }
  }

  abstract void start();

  boolean isDebugEnabled() {
    return log.isDebugEnabled();
  }

  boolean isWarningEnabled() {
    return log.isWarnEnabled();
  }
}
