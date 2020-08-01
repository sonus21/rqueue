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

package com.github.sonus21.rqueue.models.response;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.github.sonus21.rqueue.exception.ErrorCode;
import com.github.sonus21.rqueue.models.SerializableBase;
import javax.validation.constraints.NotNull;
import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.ToString;

@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@ToString
@EqualsAndHashCode(callSuper = false)
public class CodeAndMessage extends SerializableBase {
  private static final long serialVersionUID = 9182049559801361646L;
  @NotNull private ErrorCode code = ErrorCode.SUCCESS;
  private String message;

  public CodeAndMessage(ErrorCode code) {
    this(code, code.getMessage());
  }

  @JsonIgnore
  public void set(ErrorCode errorCode, String message) {
    this.code = errorCode;
    this.message = message;
  }

  @JsonIgnore
  public void set(ErrorCode errorCode) {
    set(errorCode, errorCode.getMessage());
  }
}
