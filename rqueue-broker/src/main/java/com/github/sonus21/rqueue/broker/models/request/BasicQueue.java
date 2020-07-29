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

package com.github.sonus21.rqueue.broker.models.request;

import com.github.sonus21.rqueue.models.SerializableBase;
import javax.validation.constraints.NotEmpty;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.ToString;

@NoArgsConstructor
@Getter
@Setter
@ToString
public class BasicQueue extends SerializableBase {

  private static final long serialVersionUID = 7186553210053429559L;
  @NotEmpty private String name;

  @Override
  public boolean equals(Object other) {
    if (!(other instanceof BasicQueue)) {
      return false;
    }
    return ((BasicQueue) other).name.equals(name);
  }

  @Override
  public int hashCode() {
    return name.hashCode();
  }
}
