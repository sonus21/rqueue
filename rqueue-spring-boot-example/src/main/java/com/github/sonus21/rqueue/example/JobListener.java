package com.github.sonus21.rqueue.example;

import com.github.sonus21.rqueue.annotation.RqueueHandler;
import com.github.sonus21.rqueue.annotation.RqueueListener;
import org.springframework.stereotype.Component;

@RqueueListener(
    value = "job-queue",
    deadLetterQueue = "job-morgue",
    numRetries = "2",
    deadLetterQueueListenerEnabled = "false",
    concurrency = "10-20")
@Component
public class JobListener extends BaseListener {

  @RqueueHandler
  public void onJobMessage(Job job) {
    execute("job-queue: {}", job, true);
  }

  @RqueueHandler(primary = true)
  public void onLinkedInJobMessage(Job job) {
    execute("linkedin:job-queue: {}", job, true);
  }
}
