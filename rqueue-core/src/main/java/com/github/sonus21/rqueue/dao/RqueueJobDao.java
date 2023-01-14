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

package com.github.sonus21.rqueue.dao;

import com.github.sonus21.rqueue.models.db.RqueueJob;
import java.time.Duration;
import java.util.Collection;
import java.util.List;

public interface RqueueJobDao {

  void createJob(RqueueJob rqueueJob, Duration expiry);

  void save(RqueueJob rqueueJob, Duration expiry);

  RqueueJob findById(String jobId);

  List<RqueueJob> findJobsByIdIn(Collection<String> jobIds);

  List<RqueueJob> finByMessageIdIn(List<String> messageIds);

  List<RqueueJob> finByMessageId(String messageId);

  void delete(String jobId);
}
