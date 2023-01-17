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

package com.github.sonus21.rqueue.spring.services;

import com.github.sonus21.rqueue.test.entity.FailureDetail;
import com.github.sonus21.rqueue.test.repository.FailureDetailRepository;
import jakarta.persistence.EntityManager;
import jakarta.persistence.EntityManagerFactory;
import jakarta.persistence.criteria.CriteriaBuilder;
import jakarta.persistence.criteria.CriteriaQuery;
import jakarta.persistence.criteria.Root;
import java.util.List;
import java.util.Optional;
import lombok.AllArgsConstructor;
import org.hibernate.Session;
import org.hibernate.Transaction;
import org.springframework.stereotype.Repository;
import org.springframework.util.CollectionUtils;

@Repository
@AllArgsConstructor
public class FailureDetailRepositoryImpl implements FailureDetailRepository {

  private final EntityManagerFactory entityManagerFactory;

  @Override
  public <S extends FailureDetail> S save(S entity) {
    EntityManager entityManager = entityManagerFactory.createEntityManager();
    Session session = entityManager.unwrap(Session.class);
    Transaction tx = session.beginTransaction();
    if (entity.getFailureCount() == 0) {
      session.save(entity);
    } else {
      session.update(entity);
    }
    tx.commit();
    return entity;
  }

  @Override
  public <S extends FailureDetail> Iterable<S> saveAll(Iterable<S> entities) {
    return null;
  }

  @Override
  public Optional<FailureDetail> findById(String s) {
    EntityManager em = entityManagerFactory.createEntityManager();
    CriteriaBuilder cb = em.getCriteriaBuilder();
    CriteriaQuery<FailureDetail> q = cb.createQuery(FailureDetail.class);
    Root<FailureDetail> c = q.from(FailureDetail.class);
    q.select(c).where(cb.equal(c.get("id"), s));
    List<FailureDetail> failureDetails = em.createQuery(q).getResultList();
    em.close();
    if (CollectionUtils.isEmpty(failureDetails)) {
      return Optional.empty();
    }
    return Optional.of(failureDetails.get(0));
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
  public void deleteById(String s) {
  }

  @Override
  public void delete(FailureDetail entity) {
  }

  @Override
  public void deleteAllById(Iterable<? extends String> strings) {
  }

  @Override
  public void deleteAll(Iterable<? extends FailureDetail> entities) {
  }

  @Override
  public void deleteAll() {
  }
}
