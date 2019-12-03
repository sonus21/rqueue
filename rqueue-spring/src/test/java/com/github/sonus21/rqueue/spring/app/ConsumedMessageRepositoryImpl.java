/*
 * Copyright (c) 2019-2019, Sonu Kumar
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

package com.github.sonus21.rqueue.spring.app;

import java.util.Optional;
import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import lombok.AllArgsConstructor;
import org.hibernate.Session;
import org.springframework.stereotype.Repository;
import rqueue.test.entity.ConsumedMessage;
import rqueue.test.repository.ConsumedMessageRepository;

@Repository
@AllArgsConstructor
public class ConsumedMessageRepositoryImpl implements ConsumedMessageRepository {
  private final EntityManagerFactory entityManagerFactory;

  @Override
  public <S extends ConsumedMessage> S save(S entity) {
    EntityManager entityManager = entityManagerFactory.createEntityManager();
    Session session = entityManager.unwrap(Session.class);
    session.save(entity);
    return entity;
  }

  @Override
  public <S extends ConsumedMessage> Iterable<S> saveAll(Iterable<S> entities) {
    return null;
  }

  @Override
  public Optional<ConsumedMessage> findById(String s) {
    return Optional.empty();
  }

  @Override
  public boolean existsById(String s) {
    return false;
  }

  @Override
  public Iterable<ConsumedMessage> findAll() {
    return null;
  }

  @Override
  public Iterable<ConsumedMessage> findAllById(Iterable<String> strings) {
    return null;
  }

  @Override
  public long count() {
    return 0;
  }

  @Override
  public void deleteById(String s) {}

  @Override
  public void delete(ConsumedMessage entity) {}

  @Override
  public void deleteAll(Iterable<? extends ConsumedMessage> entities) {}

  @Override
  public void deleteAll() {}
}
