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

package com.github.sonus21.test;

import java.util.Vector;
import java.util.concurrent.Future;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

public class TestTaskExecutor extends ThreadPoolTaskExecutor {

  private static final long serialVersionUID = 8310240227553949352L;
  private final boolean useResources;
  private final Vector<Runnable> submittedTasks = new Vector<>();
  private final Vector<Runnable> executedTasks = new Vector<>();

  public TestTaskExecutor() {
    this(true);
  }

  public TestTaskExecutor(boolean useResources) {
    this.useResources = useResources;
  }

  @Override
  public Future<?> submit(Runnable task) {
    submittedTasks.add(task);
    if (useResources) {
      return super.submit(task);
    }
    return null;
  }

  @Override
  public void execute(Runnable r) {
    executedTasks.add(r);
    if (useResources) {
      super.execute(r);
    }
  }

  public int getSubmittedTaskCount() {
    return submittedTasks.size();
  }
}
