/*
 * Copyright (c) 2019-2023 Sonu Kumar
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

package com.github.sonus21.rqueue.spring.boot.application;

import com.github.sonus21.rqueue.test.application.BaseApplicationWithBackoff;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.PropertySource;
import org.springframework.data.jpa.repository.config.EnableJpaRepositories;
import org.springframework.transaction.annotation.EnableTransactionManagement;

@PropertySource("classpath:application.properties")
@SpringBootApplication(scanBasePackages = {"com.github.sonus21.rqueue.test"})
@EnableJpaRepositories(basePackages = {"com.github.sonus21.rqueue.test.repository"})
@EnableTransactionManagement
public class ApplicationWithTaskExecutionBackoff extends BaseApplicationWithBackoff {

  private static volatile ConfigurableApplicationContext context;
  private static ClassLoader mainThreadClassLoader;

  public static void main(String[] args) {
    mainThreadClassLoader = Thread.currentThread().getContextClassLoader();
    context = SpringApplication.run(ApplicationWithTaskExecutionBackoff.class, args);
  }

  public static void restart() {
    ApplicationArguments args = context.getBean(ApplicationArguments.class);
    Thread thread =
        new Thread(
            () -> {
              context.close();
              context = SpringApplication.run(Application.class, args.getSourceArgs());
            });
    thread.setContextClassLoader(mainThreadClassLoader);
    thread.setDaemon(false);
    thread.start();
  }
}
