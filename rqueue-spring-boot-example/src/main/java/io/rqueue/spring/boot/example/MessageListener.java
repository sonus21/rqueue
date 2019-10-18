package io.rqueue.spring.boot.example;

import io.rqueue.annotation.RqueueListener;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

@Component
@Slf4j
public class MessageListener {
  @RqueueListener(value = "${rqueue.test.queue}")
  public void consumeMessage(Object message) {
    log.info("test: {}", message);
    // throw new NullPointerException("Failing");
  }

  @RqueueListener(
      value = {"${rqueue.dtest.queue}", "${rqueue.dtest2.queue}"},
      delayedQueue = "${rqueue.dtest.queue.delayed-queue}",
      numRetries = "${rqueue.dtest.queue.retries}")
  public void onMessage(Object message) {
    log.info("dtest: {}", message);
    throw new NullPointerException(message.toString());
  }

  @RqueueListener(value = "job-queue", delayedQueue = "true")
  public void onMessage(Job job) {
    log.info("job-queue: {}", job);
  }
}
