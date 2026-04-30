/*
 * Copyright (c) 2024-2026 Sonu Kumar
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

package com.github.sonus21.rqueue.listener;

import com.github.sonus21.rqueue.core.RqueueMessage;
import com.github.sonus21.rqueue.core.spi.MessageBroker;
import com.github.sonus21.rqueue.core.support.RqueueMessageUtils;
import com.github.sonus21.rqueue.listener.RqueueMessageListenerContainer.QueueStateMgr;
import com.github.sonus21.rqueue.utils.backoff.TaskExecutionBackOff;
import java.lang.reflect.Method;
import java.time.Duration;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.messaging.converter.MessageConverter;
import org.springframework.messaging.handler.HandlerMethod;

/**
 * Per-listener poller used by capability-gated broker backends (currently NATS / JetStream).
 *
 * <p>Instances are created when the active {@link MessageBroker#capabilities()} reports
 * {@code usesPrimaryHandlerDispatch == false}. One poller is bound to a single
 * {@code (queueDetail, consumerName, handlerMethod)} triple and runs an independent loop:
 *
 * <ol>
 *   <li>{@link MessageBroker#pop} a batch with a short wait;
 *   <li>for each message: deserialize the JSON payload via the configured
 *       {@link MessageConverter} into the handler method's first parameter type, then invoke
 *       the bound bean method via reflection;
 *   <li>{@link MessageBroker#ack} on success;
 *   <li>{@link MessageBroker#nack} with a backoff delay on exception.
 * </ol>
 *
 * <h2>Design choices (v1, Phase 3.5)</h2>
 *
 * <p><b>Direct reflection dispatch (Option B).</b> Each poller already has a single resolved
 * {@link HandlerMethod}, so the broker path bypasses {@link RqueueMessageHandler}'s
 * destination-based mapping entirely. This avoids the primary/secondary dispatch logic that
 * is not honored by NATS-style backends and keeps the runtime path narrow. The trade-off is
 * that Spring messaging argument resolvers (headers, {@code Message<T>} wrapping, principals)
 * are not consulted. The first method parameter receives the deserialized payload; richer
 * argument resolution is deferred to a future phase.
 *
 * <p><b>Single-thread poller per (queue, consumerName).</b> {@code @RqueueListener.concurrency}
 * is not honored on the broker path in v1. JetStream's MaxAckPending already controls
 * in-flight distribution; if a user sets concurrency &gt; 1, a single INFO is logged.
 */
final class BrokerMessagePoller implements Runnable {

  private static final Logger log = LoggerFactory.getLogger(BrokerMessagePoller.class);
  private static final int DEFAULT_BATCH = 8;
  private static final Duration DEFAULT_FETCH_WAIT = Duration.ofMillis(500);
  private static final long ERROR_BACKOFF_MS = 200L;

  private final MessageBroker broker;
  private final QueueDetail queueDetail;
  private final String consumerName;
  private final HandlerMethod handlerMethod;
  private final MessageConverter messageConverter;
  private final TaskExecutionBackOff backoff;
  private final QueueStateMgr queueStateMgr;
  private final int batchSize;
  private final Duration fetchWait;
  private volatile boolean running = true;

  BrokerMessagePoller(
      MessageBroker broker,
      QueueDetail queueDetail,
      String consumerName,
      HandlerMethod handlerMethod,
      MessageConverter messageConverter,
      TaskExecutionBackOff backoff,
      QueueStateMgr queueStateMgr) {
    this(
        broker,
        queueDetail,
        consumerName,
        handlerMethod,
        messageConverter,
        backoff,
        queueStateMgr,
        Math.max(1, queueDetail.getBatchSize() > 0 ? queueDetail.getBatchSize() : DEFAULT_BATCH),
        DEFAULT_FETCH_WAIT);
  }

  BrokerMessagePoller(
      MessageBroker broker,
      QueueDetail queueDetail,
      String consumerName,
      HandlerMethod handlerMethod,
      MessageConverter messageConverter,
      TaskExecutionBackOff backoff,
      QueueStateMgr queueStateMgr,
      int batchSize,
      Duration fetchWait) {
    this.broker = broker;
    this.queueDetail = queueDetail;
    this.consumerName = consumerName;
    this.handlerMethod = handlerMethod;
    this.messageConverter = messageConverter;
    this.backoff = backoff;
    this.queueStateMgr = queueStateMgr;
    this.batchSize = batchSize;
    this.fetchWait = fetchWait;
  }

  /** Signal the loop to exit at the next iteration boundary. */
  void stop() {
    this.running = false;
  }

  boolean isRunning() {
    return running;
  }

  String getConsumerName() {
    return consumerName;
  }

  QueueDetail getQueueDetail() {
    return queueDetail;
  }

  @Override
  public void run() {
    log.info(
        "BrokerMessagePoller starting queue='{}' consumerName='{}' batch={}",
        queueDetail.getName(),
        consumerName,
        batchSize);
    while (running) {
      try {
        if (queueStateMgr != null && queueStateMgr.isQueuePaused(queueDetail.getName())) {
          sleepQuietly(fetchWait.toMillis());
          continue;
        }
        List<RqueueMessage> msgs = broker.pop(queueDetail, consumerName, batchSize, fetchWait);
        if (msgs == null || msgs.isEmpty()) {
          continue;
        }
        for (RqueueMessage msg : msgs) {
          if (!running) {
            return;
          }
          dispatch(msg);
        }
      } catch (Exception e) {
        if (e instanceof InterruptedException) {
          Thread.currentThread().interrupt();
          log.info(
              "BrokerMessagePoller interrupted queue='{}' consumerName='{}'",
              queueDetail.getName(),
              consumerName);
          return;
        }
        log.error(
            "BrokerMessagePoller poll loop error queue='{}' consumerName='{}': {}",
            queueDetail.getName(),
            consumerName,
            e.toString(),
            e);
        sleepQuietly(ERROR_BACKOFF_MS);
      }
    }
    log.info(
        "BrokerMessagePoller stopped queue='{}' consumerName='{}'",
        queueDetail.getName(),
        consumerName);
  }

  private void dispatch(RqueueMessage msg) {
    Object payload;
    try {
      payload = RqueueMessageUtils.convertMessageToObject(msg, messageConverter);
    } catch (Exception conversion) {
      log.error(
          "Failed to convert payload queue='{}' messageId='{}': {}",
          queueDetail.getName(),
          msg.getId(),
          conversion.toString(),
          conversion);
      // Cannot deserialize; nack with a small delay so JetStream max-deliver can DLQ.
      long delay = computeBackoff(null, msg, msg.getFailureCount() + 1, conversion);
      safeNack(msg, delay);
      return;
    }
    try {
      invokeHandler(payload);
      broker.ack(queueDetail, msg);
    } catch (Throwable t) {
      log.warn(
          "Handler invocation failed queue='{}' messageId='{}' consumerName='{}': {}",
          queueDetail.getName(),
          msg.getId(),
          consumerName,
          t.toString(),
          t);
      long delay = computeBackoff(payload, msg, msg.getFailureCount() + 1, t);
      safeNack(msg, delay);
    }
  }

  private long computeBackoff(Object payload, RqueueMessage msg, int failureCount, Throwable t) {
    if (backoff == null) {
      return 0L;
    }
    try {
      long d = backoff.nextBackOff(payload, msg, failureCount, t);
      return Math.max(0L, d == TaskExecutionBackOff.STOP ? 0L : d);
    } catch (Exception e) {
      log.warn("Backoff computation failed: {}", e.toString(), e);
      return 0L;
    }
  }

  private void safeNack(RqueueMessage msg, long delayMs) {
    try {
      broker.nack(queueDetail, msg, delayMs);
    } catch (Exception e) {
      log.error(
          "nack failed queue='{}' messageId='{}': {}",
          queueDetail.getName(),
          msg.getId(),
          e.toString(),
          e);
    }
  }

  private void invokeHandler(Object payload) throws Exception {
    Method method = handlerMethod.getMethod();
    Object bean = handlerMethod.getBean();
    if (!method.canAccess(bean)) {
      method.setAccessible(true);
    }
    int paramCount = method.getParameterCount();
    Object[] args;
    if (paramCount == 0) {
      args = new Object[0];
    } else if (paramCount == 1) {
      args = new Object[] {payload};
    } else {
      Object[] padded = new Object[paramCount];
      padded[0] = payload;
      args = padded;
    }
    try {
      method.invoke(bean, args);
    } catch (java.lang.reflect.InvocationTargetException ite) {
      Throwable cause = ite.getCause();
      if (cause instanceof Exception) {
        throw (Exception) cause;
      }
      if (cause instanceof Error) {
        throw (Error) cause;
      }
      throw ite;
    }
  }

  private static void sleepQuietly(long ms) {
    if (ms <= 0) {
      return;
    }
    try {
      Thread.sleep(ms);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  }
}
