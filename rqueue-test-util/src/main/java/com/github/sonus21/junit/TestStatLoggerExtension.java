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

package com.github.sonus21.junit;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.extension.AfterAllCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.TestWatcher;

@Slf4j
public class TestStatLoggerExtension implements TestWatcher, AfterAllCallback {

  private final List<TestResultStatus> testResultsStatus = new ArrayList<>();

  @Override
  public void testDisabled(ExtensionContext context, Optional<String> reason) {
    log.info(
        "Test Disabled for test {}: with reason :- {}",
        context.getDisplayName(),
        reason.orElse("No reason"));

    testResultsStatus.add(TestResultStatus.DISABLED);
  }

  @Override
  public void testSuccessful(ExtensionContext context) {
    log.info("Test Successful for test {}: ", context.getDisplayName());
    testResultsStatus.add(TestResultStatus.SUCCESSFUL);
  }

  @Override
  public void testAborted(ExtensionContext context, Throwable cause) {
    log.info("Test Aborted for test {}: ", context.getDisplayName());
    testResultsStatus.add(TestResultStatus.ABORTED);
  }

  @Override
  public void testFailed(ExtensionContext context, Throwable cause) {
    log.info("Test Aborted for test {}: ", context.getDisplayName());
    testResultsStatus.add(TestResultStatus.FAILED);
  }

  @Override
  public void afterAll(ExtensionContext context) throws Exception {
    Map<TestResultStatus, Long> summary =
        testResultsStatus.stream()
            .collect(Collectors.groupingBy(Function.identity(), Collectors.counting()));

    log.info("Test result summary for {} {}", context.getDisplayName(), summary.toString());
  }

  private enum TestResultStatus {
    SUCCESSFUL,
    ABORTED,
    FAILED,
    DISABLED
  }
}
