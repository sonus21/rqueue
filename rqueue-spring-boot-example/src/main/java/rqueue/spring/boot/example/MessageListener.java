package rqueue.spring.boot.example;

import com.github.sonus21.rqueue.annotation.RqueueListener;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

@Component
@Slf4j
public class MessageListener {
  @RqueueListener(value = "${rqueue.test.queue}")
  public void consumeMessage(String message) {
    log.info("test: {}", message.getClass());
    // throw new NullPointerException("Failing");
  }

  @RqueueListener(
      value = {"${rqueue.dtest.queue}", "${rqueue.dtest2.queue}"},
      delayedQueue = "${rqueue.dtest.queue.delayed-queue}",
      numRetries = "${rqueue.dtest.queue.retries}")
  public void onMessage(String message) {
    log.info("dtest: {}", message);
    throw new NullPointerException(message.toString());
  }

  @RqueueListener(value = "job-queue", delayedQueue = "true")
  public void onMessage(Job job) {
    log.info("job-queue: {}", job);
  }
}
