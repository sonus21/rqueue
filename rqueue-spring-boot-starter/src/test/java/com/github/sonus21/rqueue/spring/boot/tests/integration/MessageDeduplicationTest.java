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

package com.github.sonus21.rqueue.spring.boot.tests.integration;

import com.github.sonus21.rqueue.spring.boot.application.Application;
import com.github.sonus21.rqueue.test.common.SpringTestBase;
import com.github.sonus21.test.RqueueSpringTestRunner;
import lombok.extern.slf4j.Slf4j;
import org.junit.Ignore;
import org.junit.runner.RunWith;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestPropertySource;

@SpringBootTest
@RunWith(RqueueSpringTestRunner.class)
@ContextConfiguration(classes = Application.class)
@Slf4j
@TestPropertySource(
    properties = {
      "rqueue.retry.per.poll=20",
      "rqueue.scheduler.auto.start=true",
      "spring.redis.port=8009",
      "mysql.db.name=MessageDeduplicationTest",
      "rqueue.metrics.count.failure=false",
      "rqueue.metrics.count.execution=false",
    })
@Ignore
public class MessageDeduplicationTest extends SpringTestBase {}
