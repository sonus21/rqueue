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

package com.github.sonus21.rqueue.test.tests;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.sonus21.rqueue.exception.TimedOutException;
import org.junit.Before;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;
import org.springframework.web.context.WebApplicationContext;

public abstract class SpringWebTestBase extends SpringTestBase {
  @Autowired protected WebApplicationContext wac;
  protected MockMvc mockMvc;
  protected ObjectMapper mapper = new ObjectMapper();

  @Before
  public void init() throws TimedOutException {
    this.mockMvc = MockMvcBuilders.webAppContextSetup(this.wac).build();
  }
}
