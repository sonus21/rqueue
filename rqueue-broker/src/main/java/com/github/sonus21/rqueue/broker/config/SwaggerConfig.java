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

package com.github.sonus21.rqueue.broker.config;

import static springfox.documentation.builders.PathSelectors.regex;

import com.github.sonus21.rqueue.broker.service.utils.AuthUtils;
import com.google.common.collect.Lists;
import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import springfox.documentation.builders.ApiInfoBuilder;
import springfox.documentation.builders.PathSelectors;
import springfox.documentation.service.ApiInfo;
import springfox.documentation.service.ApiKey;
import springfox.documentation.service.AuthorizationScope;
import springfox.documentation.service.Contact;
import springfox.documentation.service.SecurityReference;
import springfox.documentation.spi.DocumentationType;
import springfox.documentation.spi.service.contexts.SecurityContext;
import springfox.documentation.spring.web.plugins.Docket;
import springfox.documentation.swagger2.annotations.EnableSwagger2;

@Configuration
@EnableSwagger2
@Slf4j
public class SwaggerConfig {
  private static final String DEFAULT_INCLUDE_PATTERN = "/api/.*";

  @Bean
  public Docket api() {
    Docket docket =
        new Docket(DocumentationType.SWAGGER_2)
            .apiInfo(apiInfo())
            .pathMapping("/")
            .forCodeGeneration(true)
            .securityContexts(Lists.newArrayList(securityContext()))
            .securitySchemes(Lists.newArrayList(apiKey()))
            .useDefaultResponseMessages(false);
    docket = docket.select().paths(regex(DEFAULT_INCLUDE_PATTERN)).build();
    return docket;
  }

  private ApiKey apiKey() {
    return new ApiKey("Basic Authorization", AuthUtils.AUTHORIZATION_HEADER, "header");
  }

  private ApiInfo apiInfo() {
    Contact contact =
        new Contact("Sonu Kumar", "https://github.com/sonus21", "sonunitw12@gmail.com");
    return new ApiInfoBuilder()
        .title("Rqueue Broker API")
        .description("Redis Baked Message Broker - API")
        .version("0.1.0")
        .licenseUrl("https://github.com/sonus21/rqueue/blob/master/LICENSE")
        .contact(contact)
        .license("Apache 2.0")
        .termsOfServiceUrl("https://github.com/sonus21/rqueue/blob/master/LICENSE")
        .build();
  }

  private SecurityContext securityContext() {
    return SecurityContext.builder()
        .securityReferences(defaultAuth())
        .forPaths(PathSelectors.regex(DEFAULT_INCLUDE_PATTERN))
        .build();
  }

  private List<SecurityReference> defaultAuth() {
    AuthorizationScope authorizationScope = new AuthorizationScope("global", "accessEverything");
    AuthorizationScope[] authorizationScopes = new AuthorizationScope[1];
    authorizationScopes[0] = authorizationScope;
    return Lists.newArrayList(new SecurityReference("Basic Authorization", authorizationScopes));
  }
}
