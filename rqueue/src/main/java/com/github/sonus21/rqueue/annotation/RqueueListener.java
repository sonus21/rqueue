/*
 * Copyright 2019 Sonu Kumar
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

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Annotation for mapping a method onto message-handling methods by matching to the message queue.
 * Any method can be marked with with this annotation, different field can be configured in any
 * order.
 */
@Target(ElementType.METHOD)
@Retention(RetentionPolicy.RUNTIME)
@Documented
public @interface RqueueListener {
  /**
   * List of queues. Queues can be defined by their name or placeholder that would be resolved to
   * properties file.
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
   * how many times a message should be retried before it can be discarded or send it to DLQ in case
   * of consecutive failures. Whenever message handler fails, container keeps retrying to deliver
   * the same message but in some cases we can ignore the failure or discard this message.
   *
   * @return integer value
   */
  String numRetries() default "-1";

  /**
   * In case of multiple failures corresponding message would be stored in this queue
   *
   * @return dead letter queue name
   */
  String deadLetterQueue() default "";
}
