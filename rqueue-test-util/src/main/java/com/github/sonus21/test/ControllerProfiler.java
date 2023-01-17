/*
 * Copyright (c) 2020-2023 Sonu Kumar
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

package com.github.sonus21.test;

import lombok.extern.slf4j.Slf4j;
import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.annotation.Around;
import org.aspectj.lang.annotation.Aspect;
import org.aspectj.lang.annotation.Pointcut;

@Aspect
@Slf4j
public class ControllerProfiler {

  @Pointcut(
      "execution(* com.github.sonus21.rqueue.*.controller..*.*(..))||execution(* com.github.sonus21.rqueue.*.Controller.*(..))")
  public void controller() {
  }

  @Around("controller()")
  public Object profile(ProceedingJoinPoint pjp) throws Throwable {
    long startTime = System.currentTimeMillis();
    Object response = pjp.proceed();
    log.info(
        " {} {} Time: {} ms",
        pjp.getTarget().getClass().getName(),
        pjp.getSignature().getName(),
        System.currentTimeMillis() - startTime);
    return response;
  }
}
