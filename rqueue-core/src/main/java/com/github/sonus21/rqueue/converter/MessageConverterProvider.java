/*
 * Copyright (c) 2021-2023 Sonu Kumar
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

package com.github.sonus21.rqueue.converter;

import com.github.sonus21.rqueue.core.DefaultRqueueMessageConverter;
import org.springframework.messaging.Message;
import org.springframework.messaging.converter.CompositeMessageConverter;
import org.springframework.messaging.converter.MappingJackson2MessageConverter;
import org.springframework.messaging.converter.MessageConverter;
import org.springframework.messaging.converter.SmartMessageConverter;
import org.springframework.messaging.converter.StringMessageConverter;

/**
 * MessageConverterProvider provides an interface to return message converter that will be used
 * across Rqueue.
 *
 * <p>Using message converter using other way is discouraged as it can cause problem in some flow.
 *
 * <p><b>NOTE:</b> Any custom implementation can expect targetClass being null, if targetClass is
 * null then it can return the string it self. See implementation of
 * {@link JsonMessageConverter#fromMessage(Message, Class)}
 *
 * @see JsonMessageConverter
 * @see GenericMessageConverter
 * @see StringMessageConverter
 * @see CompositeMessageConverter
 * @see SmartMessageConverter
 * @see DefaultRqueueMessageConverter
 * @see MappingJackson2MessageConverter
 */
public interface MessageConverterProvider {

  /**
   * The returned message converter can be of any type
   *
   * @return MessageConverter object
   */
  MessageConverter getConverter();
}
