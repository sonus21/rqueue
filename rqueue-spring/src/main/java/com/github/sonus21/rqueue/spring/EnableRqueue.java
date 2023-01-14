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

package com.github.sonus21.rqueue.spring;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import org.springframework.context.annotation.Import;

/**
 * This annotation can be used to auto configure Rqueue library by providing some sample
 * configuration like by just providing
 * {@link org.springframework.data.redis.connection.RedisConnectionFactory}.
 *
 * <p>All other beans would be created automatically. Though other components of library can be
 * configured as well using
 * {@link com.github.sonus21.rqueue.config.SimpleRqueueListenerContainerFactory}. Even it can be
 * configured at very fine-grained level by creating all individual beans created in
 * {@link RqueueListenerConfig}
 */
@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
@Import({RqueueListenerConfig.class})
public @interface EnableRqueue {

}
