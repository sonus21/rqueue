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

package com.github.sonus21.rqueue.core;

import com.github.sonus21.rqueue.exception.QueueDoesNotExist;
import com.github.sonus21.rqueue.listener.QueueDetail;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

public class QueueRegistry {
  QueueRegistry() {}

  private static Map<String, QueueDetail> queueNameToDetail = new HashMap<>();
  private static final Object lock = new Object();

  public static QueueDetail get(String queueName) {
    QueueDetail queueDetail = queueNameToDetail.get(queueName);
    if (queueDetail == null) {
      throw new QueueDoesNotExist(queueName);
    }
    return queueDetail;
  }

  public static boolean isRegistered(String queueName) {
    return queueNameToDetail.containsKey(queueName);
  }

  public static void register(QueueDetail queueDetail) {
    synchronized (lock) {
      queueNameToDetail.put(queueDetail.getName(), queueDetail);
      lock.notifyAll();
    }
  }

  public static void delete() {
    synchronized (lock) {
      queueNameToDetail.clear();
      lock.notifyAll();
    }
  }

  public static void register(Collection<QueueDetail> queueDetails) {
    synchronized (lock) {
      for (QueueDetail queueDetail : queueDetails) {
        queueNameToDetail.put(queueDetail.getName(), queueDetail);
      }
      lock.notifyAll();
    }
  }

  public static List<String> getQueues() {
    return Collections.unmodifiableList(new LinkedList<>(queueNameToDetail.keySet()));
  }

  public static List<QueueDetail> getQueueDetails() {
    return Collections.unmodifiableList(new LinkedList<>(queueNameToDetail.values()));
  }

  public static Map<String, QueueDetail> getQueueMap() {
    return Collections.unmodifiableMap(queueNameToDetail);
  }

  public static int getQueueCount() {
    return queueNameToDetail.size();
  }
}
