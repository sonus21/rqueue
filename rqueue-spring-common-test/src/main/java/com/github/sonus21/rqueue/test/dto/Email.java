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

package com.github.sonus21.rqueue.test.dto;

import java.util.UUID;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.ToString;
import org.apache.commons.lang3.RandomStringUtils;

@Getter
@Setter
@ToString(callSuper = true)
@EqualsAndHashCode(callSuper = true)
@NoArgsConstructor
public class Email extends BaseQueueMessage {

  private String email;
  private String subject;
  private String body;

  public Email(String id, String email, String subject, String body) {
    super(id);
    this.email = email;
    this.subject = subject;
    this.body = body;
  }

  public static Email newInstance() {
    String id = UUID.randomUUID().toString();
    String email = RandomStringUtils.randomAlphabetic(10).concat("@test.com");
    String subject = "Test Email " + RandomStringUtils.random(10);
    String body = "Email body" + email + id + subject;
    return new Email(id, email, subject, body);
  }
}
