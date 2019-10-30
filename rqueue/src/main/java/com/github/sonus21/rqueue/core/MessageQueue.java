/*
 * Copyright (c)  2019-2019, Sonu Kumar
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *       https://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */

package com.github.sonus21.rqueue.core;

public class MessageQueue {
  private String zsetName;
  private String queueName;
  private String channelName;

  public MessageQueue(String zsetName, String queueName, String channelName) {
    this.zsetName = zsetName;
    this.queueName = queueName;
    this.channelName = channelName;
  }

  String getZsetName() {
    return zsetName;
  }

  String getQueueName() {
    return queueName;
  }

  String getChannelName() {
    return channelName;
  }
}
