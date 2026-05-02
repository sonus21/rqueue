/*
 * Copyright (c) 2020-2026 Sonu Kumar
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

package com.github.sonus21.rqueue.web.config;

import com.github.sonus21.rqueue.utils.condition.ReactiveEnabled;
import com.github.sonus21.rqueue.utils.pebble.ResourceLoader;
import com.github.sonus21.rqueue.utils.pebble.RqueuePebbleExtension;
import io.pebbletemplates.pebble.PebbleEngine;
import io.pebbletemplates.spring.extension.SpringExtension;
import io.pebbletemplates.spring.reactive.PebbleReactiveViewResolver;
import io.pebbletemplates.spring.servlet.PebbleViewResolver;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Conditional;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.servlet.ViewResolver;

/**
 * Wires Pebble template view resolvers for the rqueue dashboard. Lives in {@code rqueue-web} so
 * headless deployments that exclude this module do not pull in Pebble or the Servlet/WebFlux view
 * stack. Picked up automatically by the existing {@code com.github.sonus21.rqueue.web} component
 * scan in {@code RqueueListenerConfig} / {@code RqueueListenerAutoConfig}.
 */
@Configuration
public class RqueueWebViewConfig {

  private static final String TEMPLATE_DIR = "templates/rqueue/";
  private static final String TEMPLATE_SUFFIX = ".html";

  private PebbleEngine createPebbleEngine() {
    ResourceLoader loader = new ResourceLoader();
    loader.setPrefix(TEMPLATE_DIR);
    loader.setSuffix(TEMPLATE_SUFFIX);
    return new PebbleEngine.Builder()
        .extension(new RqueuePebbleExtension(), new SpringExtension(null))
        .loader(loader)
        .build();
  }

  @Bean
  public ViewResolver rqueueViewResolver() {
    PebbleViewResolver resolver = new PebbleViewResolver(createPebbleEngine());
    resolver.setPrefix(TEMPLATE_DIR);
    resolver.setSuffix(TEMPLATE_SUFFIX);
    return resolver;
  }

  @Bean
  @Conditional(ReactiveEnabled.class)
  public org.springframework.web.reactive.result.view.ViewResolver reactiveRqueueViewResolver() {
    PebbleReactiveViewResolver resolver = new PebbleReactiveViewResolver(createPebbleEngine());
    resolver.setPrefix(TEMPLATE_DIR);
    resolver.setSuffix(TEMPLATE_SUFFIX);
    return resolver;
  }
}
