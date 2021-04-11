/*
 *  Copyright 2021 Sonu Kumar
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *         https://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and limitations under the License.
 *
 */

package com.github.sonus21.rqueue.test;

import com.github.sonus21.rqueue.core.support.MessageProcessor;
import java.util.ArrayList;
import java.util.List;

public class DeleteMessageListener implements MessageProcessor {
  private List<Object> messages = new ArrayList<>();

  @Override
  public boolean process(Object message) {
    messages.add(message);
    return true;
  }

  public List<Object> getMessages() {
    return messages;
  }

  public void clear() {
    this.messages = new ArrayList<>();
  }
}
