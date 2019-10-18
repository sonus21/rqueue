package com.github.sonus21.rqueue.annotation;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import org.springframework.messaging.handler.annotation.MessageMapping;

@Target(ElementType.METHOD)
@Retention(RetentionPolicy.RUNTIME)
@Documented
@MessageMapping
public @interface RqueueListener {
  /**
   * List of queues. Queues can be defined by their name.
   *
   * @return list of queues
   */
  String[] value() default {};

  /**
   * Whether this is delayed queue or not
   *
   * @return true/false
   */
  String delayedQueue() default "false";

  /**
   * num retries it needs to make in case of listener failure set this to some positive for fixed
   * retry a message would be discarded or sent ot dead later queue if it can not be consumed.
   *
   * @return integer value
   */
  String numRetries() default "-1";

  /**
   * In case of multiple failures corresponding message would be stored in this queue
   *
   * @return dead later queue name
   */
  String deadLaterQueue() default "";
}
