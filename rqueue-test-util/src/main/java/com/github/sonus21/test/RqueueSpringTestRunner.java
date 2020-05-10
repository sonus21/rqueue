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

import org.junit.runner.notification.RunNotifier;
import org.junit.runners.model.FrameworkMethod;
import org.junit.runners.model.InitializationError;
import org.springframework.test.context.TestContextManager;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

public class RqueueSpringTestRunner extends SpringJUnit4ClassRunner {

  /**
   * Construct a new {@code SpringJUnit4ClassRunner} and initialize a {@link TestContextManager} to
   * provide Spring testing functionality to standard JUnit tests.
   *
   * @param clazz the test class to be run
   * @see #createTestContextManager(Class)
   */
  public RqueueSpringTestRunner(Class<?> clazz) throws InitializationError {
    super(clazz);
  }

  /**
   * Delegate to the parent implementation for creating the test instance and then allow the {@link
   * #getTestContextManager() TestContextManager} to prepare the test instance before returning it.
   *
   * @see TestContextManager#prepareTestInstance
   */
  @Override
  protected Object createTest() throws Exception {
    Object testInstance = super.createTest();
    System.setProperty("testClass", testInstance.getClass().getSimpleName());
    return testInstance;
  }

  @Override
  protected void runChild(FrameworkMethod frameworkMethod, RunNotifier notifier) {
    System.setProperty("testName", frameworkMethod.getName());
    super.runChild(frameworkMethod, notifier);
  }
}
