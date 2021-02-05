/*
 *  Copyright 2021 Sonu Kumar
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *         https://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and limitations under the License.
 *
 */

package com.github.sonus21.rqueue.spring.services;

import com.github.sonus21.rqueue.test.entity.ConsumedMessage;
import com.github.sonus21.rqueue.test.repository.ConsumedMessageRepository;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.criteria.CriteriaBuilder;
import javax.persistence.criteria.CriteriaDelete;
import javax.persistence.criteria.CriteriaQuery;
import javax.persistence.criteria.Root;
import lombok.AllArgsConstructor;
import org.hibernate.Session;
import org.hibernate.Transaction;
import org.hibernate.query.Query;
import org.springframework.stereotype.Repository;
import org.springframework.util.CollectionUtils;

@Repository
@AllArgsConstructor
public class ConsumedMessageRepositoryImpl implements ConsumedMessageRepository {

  private final EntityManagerFactory entityManagerFactory;

  // ============================================================
  // SAVE related method
  // ==============================================================
  @Override
  public <S extends ConsumedMessage> S save(S entity) {
    EntityManager entityManager = entityManagerFactory.createEntityManager();
    Session session = entityManager.unwrap(Session.class);
    Transaction tx = session.beginTransaction();
    session.save(entity);
    tx.commit();
    entityManager.close();
    return entity;
  }

  @Override
  public <S extends ConsumedMessage> Iterable<S> saveAll(Iterable<S> entities) {
    List<ConsumedMessage> consumedMessages = new ArrayList<>();
    for (ConsumedMessage consumedMessage : entities) {
      consumedMessages.add(save(consumedMessage));
    }
    return (Iterable<S>) consumedMessages;
  }

  // ============================================================
  // FIND related method
  // ==============================================================

  @Override
  public Optional<ConsumedMessage> findById(String s) {
    Iterable<ConsumedMessage> messages = findAllById(Collections.singleton(s));
    Iterator<ConsumedMessage> it = messages.iterator();
    if (it.hasNext()) {
      return Optional.of(it.next());
    }
    return Optional.empty();
  }

  @Override
  public boolean existsById(String s) {
    return findById(s).orElse(null) == null;
  }

  private List<ConsumedMessage> executeQuery(
      EntityManager em,
      Session session,
      Root<ConsumedMessage> root,
      CriteriaQuery<ConsumedMessage> cr) {
    cr.select(root);
    Query<ConsumedMessage> query = session.createQuery(cr);
    List<ConsumedMessage> consumedMessages = query.getResultList();
    em.close();
    return consumedMessages;
  }

  @Override
  public Iterable<ConsumedMessage> findAll() {
    EntityManager entityManager = entityManagerFactory.createEntityManager();
    Session session = entityManager.unwrap(Session.class);
    CriteriaBuilder cb = session.getCriteriaBuilder();
    CriteriaQuery<ConsumedMessage> cr = cb.createQuery(ConsumedMessage.class);
    Root<ConsumedMessage> root = cr.from(ConsumedMessage.class);
    return executeQuery(entityManager, session, root, cr);
  }

  @Override
  public Iterable<ConsumedMessage> findAllById(Iterable<String> strings) {
    EntityManager entityManager = entityManagerFactory.createEntityManager();
    Session session = entityManager.unwrap(Session.class);
    CriteriaBuilder cb = session.getCriteriaBuilder();
    CriteriaQuery<ConsumedMessage> cr = cb.createQuery(ConsumedMessage.class);
    Root<ConsumedMessage> root = cr.from(ConsumedMessage.class);
    cr.where(cb.in(root.get("id").in(strings)));
    return executeQuery(entityManager, session, root, cr);
  }

  @Override
  public List<ConsumedMessage> findByQueueName(String queueName) {
    EntityManager entityManager = entityManagerFactory.createEntityManager();
    Session session = entityManager.unwrap(Session.class);
    CriteriaBuilder cb = session.getCriteriaBuilder();
    CriteriaQuery<ConsumedMessage> cr = cb.createQuery(ConsumedMessage.class);
    Root<ConsumedMessage> root = cr.from(ConsumedMessage.class);
    cr.where(cb.equal(root.get("queue_name"), queueName));
    return executeQuery(entityManager, session, root, cr);
  }

  @Override
  public List<ConsumedMessage> findByMessageId(String messageId) {
    EntityManager entityManager = entityManagerFactory.createEntityManager();
    Session session = entityManager.unwrap(Session.class);
    CriteriaBuilder cb = session.getCriteriaBuilder();
    CriteriaQuery<ConsumedMessage> cr = cb.createQuery(ConsumedMessage.class);
    Root<ConsumedMessage> root = cr.from(ConsumedMessage.class);
    cr.where(cb.equal(root.get("message_id"), messageId));
    return executeQuery(entityManager, session, root, cr);
  }

  @Override
  public List<ConsumedMessage> findByMessageIdIn(Collection<String> messageIds) {
    EntityManager entityManager = entityManagerFactory.createEntityManager();
    Session session = entityManager.unwrap(Session.class);
    CriteriaBuilder cb = session.getCriteriaBuilder();
    CriteriaQuery<ConsumedMessage> cr = cb.createQuery(ConsumedMessage.class);
    Root<ConsumedMessage> root = cr.from(ConsumedMessage.class);
    cr.where(cb.in(root.get("message_id").in(messageIds)));
    return executeQuery(entityManager, session, root, cr);
  }

  @Override
  public ConsumedMessage findByMessageIdAndTag(String messageId, String tag) {
    EntityManager entityManager = entityManagerFactory.createEntityManager();
    Session session = entityManager.unwrap(Session.class);
    CriteriaBuilder cb = session.getCriteriaBuilder();
    CriteriaQuery<ConsumedMessage> cr = cb.createQuery(ConsumedMessage.class);
    Root<ConsumedMessage> root = cr.from(ConsumedMessage.class);
    cr.where(cb.equal(root.get("message_id"), messageId)).where(cb.equal(root.get("tag"), tag));
    List<ConsumedMessage> consumedMessages = executeQuery(entityManager, session, root, cr);
    if (CollectionUtils.isEmpty(consumedMessages)) {
      return null;
    }
    return consumedMessages.get(0);
  }

  @Override
  public long count() {
    int total = 0;
    Iterable<ConsumedMessage> messages = findAll();
    for (ConsumedMessage x : messages) {
      total += 1;
    }
    return total;
  }

  // ============================================================
  // Delete related method
  // ==============================================================

  @Override
  public void deleteById(String id) {
    deleteAllByIdIn(Collections.singleton(id));
  }

  @Override
  public void delete(ConsumedMessage entity) {
    deleteById(entity.getId());
  }

  @Override
  public void deleteAll(Iterable<? extends ConsumedMessage> entities) {
    List<String> ids = new ArrayList<>();
    for (ConsumedMessage message : entities) {
      ids.add(message.getId());
    }
    deleteAllByIdIn(ids);
  }

  @Override
  public void deleteAll() {
    EntityManager entityManager = entityManagerFactory.createEntityManager();
    Session session = entityManager.unwrap(Session.class);
    CriteriaBuilder cb = session.getCriteriaBuilder();
    CriteriaDelete<ConsumedMessage> cr = cb.createCriteriaDelete(ConsumedMessage.class);
    Query<?> query = session.createQuery(cr);
    query.getSingleResult();
    entityManager.close();
  }

  @Override
  public void deleteAllByIdIn(Collection<String> ids) {
    EntityManager entityManager = entityManagerFactory.createEntityManager();
    Session session = entityManager.unwrap(Session.class);
    CriteriaBuilder cb = session.getCriteriaBuilder();
    CriteriaDelete<ConsumedMessage> cr = cb.createCriteriaDelete(ConsumedMessage.class);
    Root<ConsumedMessage> root = cr.from(ConsumedMessage.class);
    cr.where(root.get("id").in(ids));
    Query<?> query = session.createQuery(cr);
    query.getSingleResult();
    entityManager.close();
  }
}
