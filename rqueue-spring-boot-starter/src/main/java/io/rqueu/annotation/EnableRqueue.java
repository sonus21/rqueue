package io.rqueu.annotation;

import io.rqueue.spring.boot.RqueueMessageAutoConfig;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import org.springframework.context.annotation.Import;

@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
@Import({RqueueMessageAutoConfig.class})
public @interface EnableRqueue {}
