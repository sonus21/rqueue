/*
 * Copyright 2020 Sonu Kumar
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

package com.github.sonus21.rqueue.exception;

import com.github.sonus21.rqueue.models.response.BaseResponse;
import com.github.sonus21.rqueue.models.response.CodeAndMessage;
import com.github.sonus21.rqueue.models.response.FieldError;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import org.springframework.util.CollectionUtils;

@SuppressWarnings({"java:S2166", "java:S1165"})
public class ValidationException extends Throwable {

  private static final long serialVersionUID = -1144515668004621414L;
  private final ErrorCode errorCode;
  private List<FieldError> errors;

  public ValidationException(ErrorCode errorCode) {
    this.errorCode = errorCode;
    this.errors = null;
  }

  public ValidationException(FieldError error) {
    this(Collections.singletonList(error));
  }

  public ValidationException(Collection<FieldError> errors) {
    this.errorCode = null;
    this.errors = new LinkedList<>(errors);
  }

  public void addError(FieldError fieldError) {
    if (this.errors == null) {
      this.errors = new LinkedList<>();
    }
    this.errors.add(fieldError);
  }

  @Override
  public String getMessage() {
    if (errorCode == null && CollectionUtils.isEmpty(errors)) {
      return "Validation error";
    }
    if (errorCode != null) {
      return errorCode.getMessage();
    }
    return errors.toString();
  }

  public BaseResponse toBaseResponse() {
    BaseResponse baseResponse = new BaseResponse(ErrorCode.VALIDATION_ERROR);
    if (!CollectionUtils.isEmpty(errors)) {
      baseResponse.setErrors(errors);
    } else if (errorCode != null) {
      FieldError fieldError =
          new FieldError(null, Collections.singletonList(new CodeAndMessage(errorCode)));
      baseResponse.setErrors(Collections.singletonList(fieldError));
    }
    if (errorCode != null) {
      baseResponse.setMessage(errorCode.getMessage());
    }
    return baseResponse;
  }
}
