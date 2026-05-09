/*
 * Copyright (c) 2026 Sonu Kumar
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * You may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 */
package com.github.sonus21.rqueue.web.service;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.Mockito.when;

import com.github.sonus21.rqueue.CoreUnitTest;
import com.github.sonus21.rqueue.dao.RqueueJobDao;
import com.github.sonus21.rqueue.exception.ProcessingException;
import com.github.sonus21.rqueue.models.db.RqueueJob;
import com.github.sonus21.rqueue.models.enums.JobStatus;
import com.github.sonus21.rqueue.models.response.DataViewResponse;
import com.github.sonus21.rqueue.serdes.RqueueSerDes;
import java.util.Arrays;
import java.util.Collections;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import reactor.core.publisher.Mono;

/**
 * Unit tests for {@link RqueueJobServiceImpl}: getJobs and getReactiveJobs paths.
 */
@CoreUnitTest
class RqueueJobServiceImplTest {

  @Mock
  private RqueueJobDao rqueueJobDao;

  @Mock
  private RqueueSerDes rqueueSerDes;

  private RqueueJobServiceImpl service;

  @BeforeEach
  void setUp() {
    service = new RqueueJobServiceImpl(rqueueJobDao, rqueueSerDes);
  }

  private RqueueJob job(String id, String messageId, long createdAt, long updatedAt) {
    RqueueJob j = new RqueueJob();
    j.setId(id);
    j.setMessageId(messageId);
    j.setCreatedAt(createdAt);
    j.setUpdatedAt(updatedAt);
    j.setStatus(JobStatus.CREATED);
    j.setLastCheckinAt(0L);
    return j;
  }

  // ---- getJobs — empty ----

  @Test
  void getJobs_noJobs_returnsResponseWithCode0() throws ProcessingException {
    when(rqueueJobDao.finByMessageId("msg-1")).thenReturn(Collections.emptyList());
    DataViewResponse resp = service.getJobs("msg-1");
    assertEquals(0, resp.getCode());
  }

  @Test
  void getJobs_noJobs_messageIndicatesNoJobsFound() throws ProcessingException {
    when(rqueueJobDao.finByMessageId("msg-1")).thenReturn(Collections.emptyList());
    DataViewResponse resp = service.getJobs("msg-1");
    assertNotNull(resp.getMessage());
    assertEquals("No jobs found", resp.getMessage());
  }

  // ---- getJobs — single job ----

  @Test
  void getJobs_singleJob_setsHeaders() throws ProcessingException {
    RqueueJob j = job("j1", "msg-1", 1000L, 2000L);
    when(rqueueJobDao.finByMessageId("msg-1")).thenReturn(Collections.singletonList(j));

    DataViewResponse resp = service.getJobs("msg-1");

    assertNotNull(resp.getHeaders());
    assertEquals(6, resp.getHeaders().size());
  }

  @Test
  void getJobs_singleJob_hasOneRow() throws ProcessingException {
    RqueueJob j = job("j1", "msg-1", 1000L, 2000L);
    when(rqueueJobDao.finByMessageId("msg-1")).thenReturn(Collections.singletonList(j));

    DataViewResponse resp = service.getJobs("msg-1");

    assertNotNull(resp.getRows());
    assertEquals(1, resp.getRows().size());
  }

  @Test
  void getJobs_singleJob_firstColumnIsJobId() throws ProcessingException {
    RqueueJob j = job("j1", "msg-1", 1000L, 2000L);
    when(rqueueJobDao.finByMessageId("msg-1")).thenReturn(Collections.singletonList(j));

    DataViewResponse resp = service.getJobs("msg-1");

    assertEquals("j1", resp.getRows().get(0).getColumns().get(0).getValue());
  }

  // ---- getJobs — sorting ----

  @Test
  void getJobs_multipleJobs_sortedByUpdatedAt() throws ProcessingException {
    RqueueJob j1 = job("j1", "msg-1", 1000L, 3000L);
    RqueueJob j2 = job("j2", "msg-1", 1000L, 1000L);
    RqueueJob j3 = job("j3", "msg-1", 1000L, 2000L);
    when(rqueueJobDao.finByMessageId("msg-1")).thenReturn(Arrays.asList(j1, j2, j3));

    DataViewResponse resp = service.getJobs("msg-1");

    assertEquals(3, resp.getRows().size());
    // Sorted ascending by updatedAt: j2(1000), j3(2000), j1(3000)
    assertEquals("j2", resp.getRows().get(0).getColumns().get(0).getValue());
    assertEquals("j1", resp.getRows().get(2).getColumns().get(0).getValue());
  }

  // ---- getJobs — with error ----

  @Test
  void getJobs_jobWithError_errorColumnPopulated() throws ProcessingException {
    RqueueJob j = job("j1", "msg-1", 1000L, 2000L);
    j.setError("something went wrong");
    when(rqueueJobDao.finByMessageId("msg-1")).thenReturn(Collections.singletonList(j));

    DataViewResponse resp = service.getJobs("msg-1");

    // Column index 3 is "Error"
    assertEquals(
        "something went wrong", resp.getRows().get(0).getColumns().get(3).getValue());
  }

  // ---- getJobs — with lastCheckinAt ----

  @Test
  void getJobs_jobWithCheckinTime_checkinColumnNotEmpty() throws ProcessingException {
    RqueueJob j = job("j1", "msg-1", 1000L, 2000L);
    j.setLastCheckinAt(System.currentTimeMillis() - 5000L);
    when(rqueueJobDao.finByMessageId("msg-1")).thenReturn(Collections.singletonList(j));

    DataViewResponse resp = service.getJobs("msg-1");

    String checkinCol = resp.getRows().get(0).getColumns().get(2).getValue().toString();
    assertNotNull(checkinCol);
    // Should contain "Ago" marker
    org.junit.jupiter.api.Assertions.assertTrue(checkinCol.contains("Ago"));
  }

  // ---- getReactiveJobs ----

  @Test
  void getReactiveJobs_wrapsGetJobsInMono() throws ProcessingException {
    when(rqueueJobDao.finByMessageId("msg-1")).thenReturn(Collections.emptyList());
    Mono<DataViewResponse> mono = service.getReactiveJobs("msg-1");
    DataViewResponse resp = mono.block();
    assertNotNull(resp);
    assertEquals(0, resp.getCode());
  }

  @Test
  void getReactiveJobs_multipleJobs_returnsCorrectCount() throws ProcessingException {
    RqueueJob j1 = job("j1", "msg-1", 1000L, 1000L);
    RqueueJob j2 = job("j2", "msg-1", 2000L, 2000L);
    when(rqueueJobDao.finByMessageId("msg-1")).thenReturn(Arrays.asList(j1, j2));

    Mono<DataViewResponse> mono = service.getReactiveJobs("msg-1");
    DataViewResponse resp = mono.block();

    assertNotNull(resp);
    assertEquals(2, resp.getRows().size());
  }
}
