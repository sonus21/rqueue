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

package com.github.sonus21.rqueue.utils;

import org.slf4j.Logger;
import org.slf4j.event.Level;

public class BaseLogger {
  private final Logger log;
  private final String groupName;

  public BaseLogger(Logger log, String groupName) {
    this.log = log;
    this.groupName = groupName;
  }

  public void log(Level level, String msg, Throwable t, Object... objects) {
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
    if (StringUtils.isEmpty(groupName)) {
      logMessage(level, msg, objects);
    } else {
      int size = objects.length + 1 + (t == null ? 0 : 1);
      Object[] objects1 = new Object[size];
      System.arraycopy(objects, 0, objects1, 1, objects.length);
      objects1[0] = groupName;
      if (t != null) {
        objects1[objects1.length - 1] = t;
      }
      String txt = "[{}] " + msg;
      logMessage(level, txt, objects1);
    }
  }

  private void logMessage(Level level, String txt, Object[] objects) {
    switch (level) {
      case INFO:
        log.info(txt, objects);
        break;
      case WARN:
        log.warn(txt, objects);
        break;
      case DEBUG:
        log.debug(txt, objects);
        break;
      case ERROR:
        log.error(txt, objects);
        break;
      default:
        log.trace(txt, objects);
    }
  }

  public boolean isDebugEnabled() {
    return log.isDebugEnabled();
  }

  public boolean isWarningEnabled() {
    return log.isWarnEnabled();
  }
}
