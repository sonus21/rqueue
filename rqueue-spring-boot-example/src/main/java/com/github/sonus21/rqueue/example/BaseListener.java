package com.github.sonus21.rqueue.example;

import com.github.sonus21.rqueue.core.RqueueMessageManager;
import com.github.sonus21.rqueue.utils.TimeoutUtils;
import java.util.Random;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Lazy;

@Slf4j
public abstract class BaseListener {

  protected static final Random random = new Random();

  @Autowired
  @Lazy
  protected RqueueMessageManager rqueueMessageManager;

  @Value("${job.fail.percentage:0}")
  protected int percentageFailure;

  @Value("${job.execution.interval:100}")
  protected int jobExecutionTime;

  protected int count;

  protected boolean shouldFail() {
    if (percentageFailure == 0) {
      return false;
    }
    if (percentageFailure >= 100) {
      return true;
    }
    return random.nextInt(100) < percentageFailure;
  }

  protected void execute(String msg, Object any, boolean failingEnabled) {
    log.info(msg, any);
    TimeoutUtils.sleep(random.nextInt(jobExecutionTime));
    if (failingEnabled && shouldFail()) {
      throw new IllegalArgumentException("Failing On Purpose " + any);
    }
  }
}
