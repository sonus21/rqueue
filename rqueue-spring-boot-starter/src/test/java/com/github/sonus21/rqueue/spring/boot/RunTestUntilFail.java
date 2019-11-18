/*
 * Copyright (c)  2019-2019, Sonu Kumar
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *       https://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */

package com.github.sonus21.rqueue.spring.boot;

import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;
import org.slf4j.Logger;

public class RunTestUntilFail implements TestRule {

  private Runnable callback;
  private int retryCount;
  private Logger logger;
  private static final int MAX_RETRY_COUNT = 10;
  private static final String ENVIRONMENT_VAR_NAME = "RETRY_COUNT";

  private void printData(String message, Object... args) {
    String msg = String.format(message, args);
    if (logger == null) {
      System.err.println(msg);
      return;
    }
    logger.info(msg);
  }

  private int getRetryCount(Integer retryCount) {
    if (retryCount != null) {
      return retryCount;
    }
    String retryCountStr = System.getenv(ENVIRONMENT_VAR_NAME);
    if (retryCountStr == null) {
      return MAX_RETRY_COUNT;
    }
    try {
      return Integer.parseInt(retryCountStr);
    } catch (NumberFormatException e) {
      printData("Environment '" + ENVIRONMENT_VAR_NAME + "' '" + retryCountStr + "' is not valid");
      return MAX_RETRY_COUNT;
    }
  }

  public RunTestUntilFail(Logger logger, Integer retryCount, Runnable failureCallback) {
    this.logger = logger;
    this.retryCount = getRetryCount(retryCount);
    this.callback = failureCallback;
  }

  public Statement apply(Statement base, Description description) {
    return statement(base, description);
  }

  private void performPostFailureActions(final Description description, int iteration) {
    printData("%s : %d run  failed", description.getDisplayName(), iteration);
    if (this.callback == null) {
      return;
    }
    callback.run();
  }

  private Statement statement(final Statement base, final Description description) {
    return new Statement() {
      @Override
      public void evaluate() throws Throwable {
        for (int i = 1; i <= retryCount; i++) {
          printData("Running Iteration: %d", i);
          try {
            base.evaluate();
          } catch (Throwable t) {
            performPostFailureActions(description, i);
            throw t;
          }
        }
        printData("%s : giving up after %s passes", description.getDisplayName(), retryCount);
      }
    };
  }
}
