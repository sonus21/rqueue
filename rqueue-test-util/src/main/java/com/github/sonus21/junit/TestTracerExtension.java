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

package com.github.sonus21.junit;

import java.lang.reflect.Method;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.springframework.test.context.junit.jupiter.SpringExtension;

public class TestTracerExtension extends SpringExtension {

  @Override
  public void beforeAll(ExtensionContext context) throws Exception {
    super.beforeAll(context);
    System.setProperty("testClass", context.getRequiredTestClass().getSimpleName());
  }

  @Override
  public void beforeEach(ExtensionContext context) throws Exception {
    super.beforeEach(context);
    Method testMethod = context.getRequiredTestMethod();
    System.setProperty("testName", testMethod.getName());
  }
}
