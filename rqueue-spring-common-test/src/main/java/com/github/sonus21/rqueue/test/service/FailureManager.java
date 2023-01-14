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

package com.github.sonus21.rqueue.test.service;

import com.github.sonus21.rqueue.test.entity.FailureDetail;
import com.github.sonus21.rqueue.test.repository.FailureDetailRepository;
import java.util.Random;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service
@RequiredArgsConstructor(onConstructor = @__(@Autowired))
public class FailureManager {

  @NonNull
  private final FailureDetailRepository failureDetailRepository;

  public boolean shouldFail(String id) {
    // no entry so no fail
    FailureDetail failureDetail = failureDetailRepository.findById(id).orElse(null);
    if (failureDetail == null) {
      return false;
    }
    // always fail
    // fail minimum number of times in a row
    if (failureDetail.getMinFailureCount() == -1
        || failureDetail.getFailureCount() < failureDetail.getMinFailureCount()) {
      failureDetail.setFailureCount(failureDetail.getFailureCount() + 1);
      failureDetailRepository.save(failureDetail);
      return true;
    }

    // exceeded failure limit
    if (failureDetail.getFailureCount() >= failureDetail.getMaxFailureCount()) {
      return false;
    }

    // random number is between min and max failure count
    Random random = new Random();
    int count = random.nextInt(failureDetail.getMaxFailureCount());
    if (count > failureDetail.getMinFailureCount()) {
      failureDetail.setFailureCount(failureDetail.getFailureCount() + 1);
      failureDetailRepository.save(failureDetail);
      return true;
    }
    return false;
  }

  public int getFailureCount(String id) {
    FailureDetail failureDetail = failureDetailRepository.findById(id).orElse(null);
    if (failureDetail == null) {
      return 0;
    }
    return failureDetail.getFailureCount();
  }

  public void createFailureDetail(String id, int minFailureCount, int maxFailureCount) {
    FailureDetail failureDetail = new FailureDetail(id, minFailureCount, maxFailureCount, 0);
    failureDetailRepository.save(failureDetail);
  }

  public void delete(String id) {
    failureDetailRepository
        .findById(id)
        .ifPresent(failureDetail -> failureDetailRepository.delete(failureDetail));
  }
}
