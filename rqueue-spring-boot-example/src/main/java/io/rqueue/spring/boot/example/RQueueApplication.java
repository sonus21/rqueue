package io.rqueue.spring.boot.example;

import io.rqueu.annotation.EnableRqueue;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.data.redis.repository.configuration.EnableRedisRepositories;

@SpringBootApplication
@EnableRedisRepositories
@EnableRqueue
public class RQueueApplication {

  public static void main(String[] args) {
    SpringApplication.run(RQueueApplication.class, args);
  }
}
