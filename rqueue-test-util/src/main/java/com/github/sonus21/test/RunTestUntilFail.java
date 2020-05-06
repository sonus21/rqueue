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

package com.github.sonus21.test;

import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;
import org.slf4j.Logger;

/**
 * Use this class to run a JUnit test until it fails, a test can be retried for any number of times
 * based on the retryCount. {@link #RunTestUntilFail(Logger log, Integer retryCount, Runnable
 * failiureCallback)}. If retryCount is not provided then it 1st looks for {@link
 * #ENVIRONMENT_VAR_NAME} into the system property then environment. If it's not available then it
 * defaults to the {@link #DEFAULT_RETRY_COUNT}. log interface is not provided then output would be
 * printed on the console otherwise it will be logged using the log interface. A failureCallback can
 * be provided that would be called whenever a test fails.
 *
 * <p>Usage
 *
 * <p>Use default configuration
 *
 * <pre>{@code
 * public class TestX{
 *  @Rule public rule = new RunTestUntilFail(null, null, null);
 *
 *  @Test
 *  public void test(){
 *      // your tests
 *   }
 * }
 * </pre>
 *
 * <p>Use retryCount and logger configuration
 *
 * <pre>{@code
 * @Sl4j
 * public class TestX{
 *  @Rule public rule = new RunTestUntilFail(log, 10, null);
 *
 *  @Test
 *  public void test(){
 *      // your tests
 *   }
 * }
 * </pre>
 *
 *
 * <p>Use retryCount and logger configuration
 *
 * <pre>{@code
 * @Sl4j
 * public class TestX{
 *  @Rule public rule = new RunTestUntilFail(log, 10, null);
 *
 *  @Test
 *  public void test(){
 *      // your tests
 *   }
 * }
 * }</pre>
 *
 *
 * {code
 *
 * <p>Using faillureCallback
 *
 * <pre>{@code
 * @Sl4j
 * public class TestX{
 *
 *  @Rule public rule = new RunTestUntilFail(log, 10, ()->{
 *    log.error("Test has failed);
 *  });
 *
 *  @Test
 *  public void test(){
 *      // your tests
 *   }
 * }
 * }</pre>
 *
 *
 */
public class RunTestUntilFail implements TestRule {

  private static final int DEFAULT_RETRY_COUNT = 3;
  private static final String ENVIRONMENT_VAR_NAME = "RETRY_COUNT";
  private Runnable callback;
  private int retryCount;
  private Logger logger;

  public RunTestUntilFail(Logger logger, Runnable failureCallback) {
    this(logger, null, failureCallback);
  }

  public RunTestUntilFail(Logger logger) {
    this(logger, null, null);
  }

  /**
   * Create a object of this class
   *
   * @param logger logger interface or null, if it's null then output will be printed ot console
   * @param retryCount how many times test has to be retried before declaring success
   * @param failureCallback runnable callback
   */
  public RunTestUntilFail(Logger logger, Integer retryCount, Runnable failureCallback) {
    this.logger = logger;
    this.retryCount = getRetryCount(retryCount);
    callback = failureCallback;
  }

  private void printData(String message, Throwable t, Object... args) {
    String msg = String.format(message, args);
    if (logger == null) {
      System.err.println(msg);
      if (t != null) {
        t.printStackTrace();
      }
      return;
    }
    logger.info(msg, t);
  }

  private int getRetryCount(Integer retryCount) {
    if (retryCount != null) {
      return retryCount;
    }
    String retryCountStr = System.getProperty(ENVIRONMENT_VAR_NAME);
    if (retryCountStr == null) {
      retryCountStr = System.getenv(ENVIRONMENT_VAR_NAME);
    }
    if (retryCountStr == null) {
      return DEFAULT_RETRY_COUNT;
    }
    try {
      return Integer.parseInt(retryCountStr);
    } catch (NumberFormatException e) {
      printData(
          "Environment '" + ENVIRONMENT_VAR_NAME + "' '" + retryCountStr + "' is not valid", null);
      return DEFAULT_RETRY_COUNT;
    }
  }

  @Override
  public Statement apply(Statement base, Description description) {
    return statement(base, description);
  }

  private void performPostFailureActions(final String testName, int iteration, Throwable t) {
    printData("************* FAILED Test: %s Iteration: %d *************", t, testName, iteration);
    if (callback == null) {
      return;
    }
    callback.run();
  }

  private Statement statement(final Statement base, final Description description) {
    return new Statement() {
      @Override
      public void evaluate() throws Throwable {
        String testName = description.getMethodName();
        int i = 1;
        do {
          printData(
              "************* RUNNING Test: %s Iteration: %d *************", null, testName, i);
          try {
            base.evaluate();
            printData(
                "************* PASS Test: %s Iteration: %d  **************", null, testName, i);
            return;
          } catch (Throwable t) {
            performPostFailureActions(testName, i, t);
            if (i == retryCount) {
              throw t;
            }
          }
          i += 1;
        } while (i < retryCount);
      }
    };
  }
}
