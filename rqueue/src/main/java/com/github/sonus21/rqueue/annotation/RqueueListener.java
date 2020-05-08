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

package com.github.sonus21.rqueue.annotation;

import com.github.sonus21.rqueue.utils.Constants;
import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Annotation for mapping a method onto message-handling methods by matching to the message queue.
 *
 * <p>Any method can be marked as listener with this annotation, different field can be configured
 * in any order as per need.
 *
 * <p>All fields support SpEL(Spring Expression Language) as well property placeholder.
 *
 * <pre>
 * &amp;Component
 * public class MessageListener {
 * &amp;RqueueListener(
 *       value="${job.queue}",
 *      delayedQueue="true",
 *      numRetries="3",
 *      deadLetterQueue="#{job.dead.letter.queue}",
 *      visibilityTimeout="30*60*1000")
 *   public void job(Job job)}{
 *      // do something
 *   }
 * }
 * </pre>
 */
@Target(ElementType.METHOD)
@Retention(RetentionPolicy.RUNTIME)
@Documented
public @interface RqueueListener {
  /**
   * List of unique queues. Queues can be defined by their name, placeholder that would be resolved
   * to properties file or could be list of comma separated queue names.
   *
   * @return list of queues.
   */
  String[] value() default {};

  // There's no use of this flag.
  @Deprecated
  String delayedQueue() default "false";

  /**
   * Number of times a message should be retried before it can be discarded or send it to dead
   * letter queue in case of consecutive failures. This is a global value for a consumer, each
   * message can have their own retries as well.
   *
   * <p>Whenever message handler fails, container keeps retrying to deliver the same message until
   * it's delivered but in some cases we can ignore or discard this message.
   *
   * <p>Default behaviour is to try to deliver the same message until it's delivered, upper limit of
   * the delivery retry is {@link Integer#MAX_VALUE} when dead letter queue is not provided. If dead
   * letter queue is provided then it will retry {@link Constants#DEFAULT_RETRY_DEAD_LETTER_QUEUE}
   * of times.
   *
   * @return integer value
   */
  String numRetries() default "-1";

  /**
   * Name of the queue, where message has to be sent in case of consecutive failures configured as
   * per {@link #numRetries()}
   *
   * @return dead letter queue name
   */
  String deadLetterQueue() default "";

  /**
   * Control visibility timeout for this/these queue(s). When a message is consumed from a queue
   * then it's hidden for other consumers for this period. This can be used to fast-recovery when a
   * job goes to running state then if it's not executed within N secs then it has to be
   * re-processed, that re-process time can be controller using this.
   *
   * <p>For example a message was consumer at 10:30AM and message was not consumed for any reason
   * like executor was shutdown, task took longer time to execute. In such cases consumed message
   * would become visible to other consumers as soon as this time elapse. By default, message would
   * become visible to other consumers after 15 minutes. In some cases 15 minutes could be too large
   * or small, in such cases We can control the visibility timeout using this field.
   *
   * <p>Minimum time is based on the two factors <br>
   * 1. Actual Task execution time <br>
   * 2. Redis call time and thread busyness.
   *
   * <p><b>NOTE:</b>If provided time is too small then same messages would be consumed by multiple
   * listeners, that can cause problem in the application. On the other-side if provided time is too
   * high then the message would be hidden from other consumers for a long time.
   *
   * <p><b>NOTE:</b> This time is in milli seconds
   *
   * @return visibilityTimeout visibility timeout
   */
  String visibilityTimeout() default "900000";

  /**
   * Control whether this listener is active or not. If this is set to false then message would not
   * be consumed by the method marked using {@link RqueueListener}.
   *
   * <p>Use case could be of like, disable this listener in a given availability zone or in a given
   * environment.
   *
   * @return whether listener is active or not.
   */
  String active() default "true";

  /**
   * Specify concurrency limits via a "lower-upper" String, e.g. "5-10", or a simple upper limit
   * String, e.g. "10" (the lower limit will be 1 in this case).
   *
   * <p>Concurrency of this consumer, number of workers used to process the message from each queue
   * If more than one queue are provided using {@link #value()} then each queue will have this
   * concurrency.
   *
   * @return concurrency for this worker.
   */
  String concurrency() default "-1";
}
