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

package com.github.sonus21.rqueue.core;

import com.github.sonus21.rqueue.exception.OverrideException;
import com.github.sonus21.rqueue.exception.QueueDoesNotExist;
import com.github.sonus21.rqueue.listener.QueueDetail;
import com.github.sonus21.rqueue.utils.PriorityUtils;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Stores all registered queue details
 *
 * <p>this stores all the queue details mapped from {@link
 * com.github.sonus21.rqueue.annotation.RqueueListener}.
 */
public final class EndpointRegistry {

  private static final Object lock = new Object();
  private static final Map<String, QueueDetail> queueNameToDetail = new HashMap<>();

  private EndpointRegistry() {
  }

  /**
   * Get QueueDetail for the given queue. If queue is having priority than it should be called with
   * {@link com.github.sonus21.rqueue.utils.PriorityUtils#getQueueNameForPriority(String, String)}.
   *
   * @param queueName queue name
   * @return queue detail
   * @throws QueueDoesNotExist this error is thrown when queue is not registered.
   * @see #get(String, String)
   */
  public static QueueDetail get(String queueName) {
    QueueDetail queueDetail = queueNameToDetail.get(queueName);
    if (queueDetail == null) {
      throw new QueueDoesNotExist(queueName);
    }
    return queueDetail;
  }

  /**
   * Get QueueDetail for the given queue, with priority
   *
   * @param queueName queue name
   * @param priority  priority of this queue like critical, high
   * @return queue detail
   * @throws QueueDoesNotExist this error is thrown when queue is not registered.
   */
  public static QueueDetail get(String queueName, String priority) {
    QueueDetail queueDetail =
        queueNameToDetail.get(PriorityUtils.getQueueNameForPriority(queueName, priority));
    if (queueDetail == null) {
      throw new QueueDoesNotExist(queueName);
    }
    return queueDetail;
  }

  public static void register(QueueDetail queueDetail) {
    synchronized (lock) {
      if (queueNameToDetail.containsKey(queueDetail.getName())) {
        throw new OverrideException(queueDetail.getName());
      }
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

  public static List<String> getActiveQueues() {
    synchronized (lock) {
      List<String> queues =
          queueNameToDetail.values().stream()
              .filter(QueueDetail::isActive)
              .map(QueueDetail::getName)
              .collect(Collectors.toList());
      lock.notifyAll();
      return queues;
    }
  }

  public static List<QueueDetail> getActiveQueueDetails() {
    synchronized (lock) {
      List<QueueDetail> queueDetails =
          queueNameToDetail.values().stream()
              .filter(QueueDetail::isActive)
              .collect(Collectors.toList());
      lock.notifyAll();
      return queueDetails;
    }
  }

  public static Map<String, QueueDetail> getActiveQueueMap() {
    synchronized (lock) {
      Map<String, QueueDetail> queueDetails =
          queueNameToDetail.values().stream()
              .filter(QueueDetail::isActive)
              .collect(Collectors.toMap(QueueDetail::getName, Function.identity()));
      lock.notifyAll();
      return queueDetails;
    }
  }

  public static String toStr() {
    StringBuilder builder = new StringBuilder();
    synchronized (lock) {
      List<QueueDetail> queueDetails = new ArrayList<>(queueNameToDetail.values());
      queueDetails.sort(Comparator.comparing(QueueDetail::getName));
      for (QueueDetail q : queueDetails) {
        builder.append(q.toString());
        builder.append("\n");
      }
      lock.notifyAll();
    }
    return builder.toString();
  }

  public static int getActiveQueueCount() {
    return getActiveQueues().size();
  }

  public static int getRegisteredQueueCount() {
    return queueNameToDetail.size();
  }
}
