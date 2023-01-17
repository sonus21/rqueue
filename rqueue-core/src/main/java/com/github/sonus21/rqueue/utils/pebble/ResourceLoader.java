/*
 * Copyright (c) 2021-2023 Sonu Kumar
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

package com.github.sonus21.rqueue.utils.pebble;

import io.pebbletemplates.pebble.error.LoaderException;
import io.pebbletemplates.pebble.loader.Loader;
import io.pebbletemplates.pebble.utils.PathUtils;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.io.UnsupportedEncodingException;
import lombok.extern.slf4j.Slf4j;
import org.springframework.core.io.ClassPathResource;

@Slf4j
public class ResourceLoader implements Loader<String> {

  private String prefix;
  private String suffix;
  private String charset = "UTF-8";

  @Override
  public Reader getReader(String templateName) {
    String location = this.getLocation(templateName);
    log.debug("Looking for template in {}.", location);
    ClassPathResource resource = new ClassPathResource(location);
    // perform the lookup
    try {
      InputStream is = resource.getInputStream();
      try {
        return new BufferedReader(new InputStreamReader(is, this.charset));
      } catch (UnsupportedEncodingException e) {
      }
    } catch (IOException e) {
      throw new LoaderException(null, "Could not find template \"" + location + "\"");
    }
    return null;
  }

  private String getLocation(String templateName) {
    return this.getPrefix() + templateName + this.getSuffix();
  }

  public String getSuffix() {
    return this.suffix;
  }

  @Override
  public void setSuffix(String suffix) {
    this.suffix = suffix;
  }

  public String getPrefix() {
    return this.prefix;
  }

  @Override
  public void setPrefix(String prefix) {
    this.prefix = prefix;
  }

  public String getCharset() {
    return this.charset;
  }

  @Override
  public void setCharset(String charset) {
    this.charset = charset;
  }

  @Override
  public String resolveRelativePath(String relativePath, String anchorPath) {
    return PathUtils.resolveRelativePath(relativePath, anchorPath, '/');
  }

  @Override
  public String createCacheKey(String templateName) {
    return templateName;
  }

  @Override
  public boolean resourceExists(String templateName) {
    return new ClassPathResource(getLocation(templateName)).exists();
  }
}
