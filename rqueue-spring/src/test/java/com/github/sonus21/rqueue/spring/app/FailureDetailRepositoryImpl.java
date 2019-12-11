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

package com.github.sonus21.rqueue.spring.app;

import java.util.Optional;
import javax.persistence.EntityManagerFactory;
import lombok.AllArgsConstructor;
import org.springframework.stereotype.Repository;
import rqueue.test.entity.FailureDetail;
import rqueue.test.repository.FailureDetailRepository;

@Repository
@AllArgsConstructor
public class FailureDetailRepositoryImpl implements FailureDetailRepository {
  private EntityManagerFactory entityManagerFactory;

  @Override
  public <S extends FailureDetail> S save(S entity) {
    return null;
  }

  @Override
  public <S extends FailureDetail> Iterable<S> saveAll(Iterable<S> entities) {
    return null;
  }

  @Override
  public Optional<FailureDetail> findById(String s) {
    return Optional.empty();
  }

  @Override
  public boolean existsById(String s) {
    return false;
  }

  @Override
  public Iterable<FailureDetail> findAll() {
    return null;
  }

  @Override
  public Iterable<FailureDetail> findAllById(Iterable<String> strings) {
    return null;
  }

  @Override
  public long count() {
    return 0;
  }

  @Override
  public void deleteById(String s) {}

  @Override
  public void delete(FailureDetail entity) {}

  @Override
  public void deleteAll(Iterable<? extends FailureDetail> entities) {}

  @Override
  public void deleteAll() {}
}
