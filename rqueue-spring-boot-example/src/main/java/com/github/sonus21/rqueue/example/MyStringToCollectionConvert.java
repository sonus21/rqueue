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

package com.github.sonus21.rqueue.example;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.CollectionFactory;
import org.springframework.core.convert.ConversionService;
import org.springframework.core.convert.TypeDescriptor;
import org.springframework.core.convert.converter.ConditionalGenericConverter;
import org.springframework.lang.Nullable;

public class MyStringToCollectionConvert implements ConditionalGenericConverter {

  @Autowired private ConversionService conversionService;

  @Override
  public Set<ConvertiblePair> getConvertibleTypes() {
    return Collections.singleton(new ConvertiblePair(String.class, Collection.class));
  }

  @Override
  public boolean matches(TypeDescriptor sourceType, TypeDescriptor targetType) {
    return (targetType.getElementTypeDescriptor() == null
        || this.conversionService.canConvert(sourceType, targetType.getElementTypeDescriptor()));
  }

  private String[] split(String string) {
    if (string == null) {
      return new String[] {};
    }
    String[] strings = string.split(",|");
    if (strings.length == 0) {
      return strings;
    }
    List<String> stringList = new ArrayList<>();
    for (int i = 0; i < strings.length; i++) {
      if (!strings[i].equals("|") && !strings[i].equals(",")) {
        stringList.add(strings[i]);
      }
    }
    return stringList.toArray(new String[0]);
  }

  @Override
  @Nullable
  public Object convert(
      @Nullable Object source, TypeDescriptor sourceType, TypeDescriptor targetType) {
    if (source == null) {
      return null;
    }
    String string = (String) source;
    String[] fields = split(string);
    TypeDescriptor elementDesc = targetType.getElementTypeDescriptor();
    Collection<Object> target =
        CollectionFactory.createCollection(
            targetType.getType(),
            (elementDesc != null ? elementDesc.getType() : null),
            fields.length);

    if (elementDesc == null) {
      for (String field : fields) {
        target.add(field.trim());
      }
    } else {
      for (String field : fields) {
        Object targetElement =
            this.conversionService.convert(field.trim(), sourceType, elementDesc);
        target.add(targetElement);
      }
    }
    return target;
  }
}
