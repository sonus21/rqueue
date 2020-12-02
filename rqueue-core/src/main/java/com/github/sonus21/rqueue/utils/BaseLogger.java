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
  private final String prefix;

  public BaseLogger(Logger log, String groupName) {
    this.log = log;
    if (StringUtils.isEmpty(groupName)) {
      this.prefix = "";
    } else {
      this.prefix = "[" + groupName + "] ";
    }
  }

  public void log(Level level, String msg, Throwable t, Object... arguments) {
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
    if (t == null) {
      logMessage(level, prefix + msg, arguments);
    } else {
      Object[] objects1 = new Object[arguments.length + 1];
      System.arraycopy(arguments, 0, objects1, 0, arguments.length);
      objects1[objects1.length - 1] = t;
      logMessage(level, prefix + msg, objects1);
    }
  }

  private void logMessage(Level level, String txt, Object... arguments) {
    switch (level) {
      case INFO:
        log.info(txt, arguments);
        break;
      case WARN:
        log.warn(txt, arguments);
        break;
      case DEBUG:
        log.debug(txt, arguments);
        break;
      case ERROR:
        log.error(txt, arguments);
        break;
      default:
        log.trace(txt, arguments);
    }
  }

  public boolean isDebugEnabled() {
    return log.isDebugEnabled();
  }

  public boolean isWarningEnabled() {
    return log.isWarnEnabled();
  }
}
