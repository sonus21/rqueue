/*
 * Copyright (c) 2020-2023 Sonu Kumar
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

import org.slf4j.Logger;
import org.slf4j.event.Level;

public class PrefixLogger {

  private final Logger log;
  private final String prefix;

  public PrefixLogger(Logger log, String groupName) {
    this.log = log;
    if (StringUtils.isEmpty(groupName)) {
      this.prefix = Constants.BLANK;
    } else {
      this.prefix = "[" + groupName + "] ";
    }
  }

  private boolean shouldLog(Level level) {
    if (level == Level.DEBUG && !log.isDebugEnabled()) {
      return false;
    }
    if (level == Level.ERROR && !log.isErrorEnabled()) {
      return false;
    }
    if (level == Level.INFO && !log.isInfoEnabled()) {
      return false;
    }
    if (level == Level.WARN && !log.isWarnEnabled()) {
      return false;
    }
    return level != Level.TRACE || log.isTraceEnabled();
  }

  public void log(Level level, String msg, Throwable t, Object... arguments) {
    if (!shouldLog(level)) {
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
