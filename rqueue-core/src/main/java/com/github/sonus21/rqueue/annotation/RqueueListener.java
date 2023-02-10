/*
 * Copyright (c) 2019-2023 Sonu Kumar
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

package com.github.sonus21.rqueue.annotation;

import com.github.sonus21.rqueue.utils.Constants;
import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Annotation for mapping a method/class onto message-handling methods by matching to the message
 * queue.
 *
 * <p>Any method can be marked as listener with this annotation, different field can be configured
 * in any order as per need.
 *
 * <p>When this is used on a class, listeners methods (public method) must be annotated by {@link
 * RqueueHandler}
 *
 * <p>All fields support SpEL(Spring Expression Language), property placeholder and constants value
 *
 * <pre>
 * &#64;Component
 * public class MessageListener {
 * &#64;RqueueListener(
 *       value="${job.queue}",
 *      numRetries="3",
 *      deadLetterQueue="#{job.dead.letter.queue}",
 *      visibilityTimeout="30*60*1000")
 *   public void job(Job job)}{
 *      // do something
 *   }
 * }
 * </pre>
 */
@Target({ElementType.METHOD, ElementType.TYPE})
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

  /**
   * Number of times a message should be retried before it can be discarded or send it to dead
   * letter queue in case of consecutive failures. This is a global value for a consumer, each
   * message can have their own retries as well.
   *
   * <p>Whenever message handler fails, container keeps retrying to deliver the same message until
   * it's delivered but in some cases we can ignore or discard this message.
   *
   * <p>Default behaviour is to try to deliver the same message until it's delivered, upper limit
   * of the delivery retry is {@link Integer#MAX_VALUE} when dead letter queue is not provided. If
   * dead letter queue is provided then it will retry
   * {@link Constants#DEFAULT_RETRY_DEAD_LETTER_QUEUE} of times.
   *
   * @return integer value
   */
  String numRetries() default "-1";

  /**
   * Name of the queue, where message has to be sent in case of consecutive failures configured as
   * per {@link #numRetries()}, by default sent message over this cannot be consumed, if you want
   * this queue to be used in the listener then set {@link #deadLetterQueueListenerEnabled()} ()}.
   *
   * @return dead letter queue name
   */
  String deadLetterQueue() default "";

  /**
   * By default, messages sent to dead letter queue are not consumable by listener. This flag is
   * used to turn on the consumable feature of dead letter queue. If this is set to true then
   * application should add message listener for the dead letter queue.
   *
   * @return true/false
   */
  String deadLetterQueueListenerEnabled() default "false";

  /**
   * Control visibility timeout for this/these queue(s). When a message is consumed from a queue
   * then it's hidden for other consumer(s) for this period. This can be used for fast-recovery when
   * a job goes to running state then if it's not executed within N secs then it will be
   * re-processed, that re-process time can be controller using this.
   *
   * <p>For example a message was consumed at 10:30AM and message was not processed successfully
   * for any reason like executor was shutdown while it's processing, task took longer time to
   * execute or application was shutdown. In such cases consumed message would become visible to
   * other consumers as soon as this time elapse. By default, message would become visible to other
   * consumers after 15 minutes, in this case it would be visible post 10:45AM. In some cases 15
   * minutes could be too large or small, in such cases we can control the visibility timeout using
   * this field.
   *
   * <p>Minimum time is based on the two factors <br>
   * 1. Actual Task execution time <br> 2. Redis call time and thread busyness.
   *
   * <p><b>NOTE:</b>If provided time is too small then same messages would be consumed by multiple
   * listeners, that will violate exactly once processing. On the other hand if provided time is too
   * high then the message would be hidden from other consumers for a longer period.
   *
   * <p><b>NOTE:</b> This time is in milliseconds
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
   * <p>Concurrency of this consumer, number of workers/threads used to process the message from
   * each queue. If more than one queue are provided using {@link #value()} then each queue will
   * have the same concurrency.
   *
   * @return concurrency for this worker.
   * @see #priority()
   */
  String concurrency() default "-1";

  /**
   * Use this to set priority of this listener. A listener can have two types of priorities.
   *
   * <p>1. Group level priority, for group level priority specify group name using {@link
   * #priorityGroup()}, if no group is provided then a default group is used.
   *
   * <p>2. Queue level priority. In the case of queue level priority, values should be provided as
   * "critical:10,high:5,medium:3,low:1", these same name must be used to enqueue message.
   *
   * <p>Priority can be any number. There are two priority control modes. 1. Strict 2. Weighted, in
   * strict priority mode queue with higher priority is preferred over other queues. In case of
   * weighted a round-robin approach is used, and weight is followed.
   *
   * @return the priority for this listener.
   * @see #priorityGroup()
   */
  String priority() default "";

  /**
   * Priority group for this listener.
   *
   * <p>More than one priority group can be configured in an application. Priority groups are
   * useful when inside a group some queue(s) have higher priority than the other queue(s).
   *
   * @return priority group name.
   */
  String priorityGroup() default "";

  /**
   * The message batch size, Rqueue will try to pull batchSize message in one Redis call. By default
   * it will try to pull one message but if concurrency is not reached then it will try to pull more
   * than one messages.
   *
   * @return batch size
   */
  String batchSize() default "-1";
}
