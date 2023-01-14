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

package com.github.sonus21.rqueue.models.request;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import com.github.sonus21.TestBase;
import com.github.sonus21.rqueue.CoreUnitTest;
import com.github.sonus21.rqueue.config.RqueueWebConfig;
import com.github.sonus21.rqueue.models.enums.DataType;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.Test;

@CoreUnitTest
class MessageMoveRequestTest extends TestBase {

  @Test
  void validationMessage() {
    MessageMoveRequest request = new MessageMoveRequest();
    assertEquals("Source cannot be empty.", request.validationMessage());
    request.setSrc("    ");
    assertEquals("Source cannot be empty.", request.validationMessage());
    request.setSrc("job-dlq");
    assertEquals("Destination cannot be empty.", request.validationMessage());
    request.setDst("   ");
    assertEquals("Destination cannot be empty.", request.validationMessage());
    request.setDst("job-dlq");
    assertEquals("Source and Destination cannot be same.", request.validationMessage());

    request.setDst("job");
    assertEquals("Source data type cannot be unknown.", request.validationMessage());

    request.setSrcType(DataType.KEY);
    assertEquals("Destination data type cannot be unknown.", request.validationMessage());

    request.setDstType(DataType.SET);
    assertEquals("Source data type is not supported.", request.validationMessage());

    request.setSrcType(DataType.LIST);
    assertEquals("Destination data type is not supported.", request.validationMessage());

    request.setDstType(DataType.LIST);
    assertNull(request.validationMessage());
  }

  @Test
  void getMessageCount() {
    RqueueWebConfig rqueueWebConfig = new RqueueWebConfig();
    rqueueWebConfig.setMaxMessageMoveCount(100);
    MessageMoveRequest request = new MessageMoveRequest();
    assertEquals(100, request.getMessageCount(rqueueWebConfig));
    Map<String, Serializable> other = new HashMap<>();
    request.setOthers(other);
    assertEquals(100, request.getMessageCount(rqueueWebConfig));

    other.put("maxMessages", 200);
    assertEquals(200, request.getMessageCount(rqueueWebConfig));
  }
}
