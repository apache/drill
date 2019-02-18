/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.drill.exec.store.syslog;

import com.fasterxml.jackson.annotation.JsonTypeName;
import com.fasterxml.jackson.annotation.JsonInclude;
import org.apache.drill.shaded.guava.com.google.common.base.Objects;
import org.apache.drill.common.logical.FormatPluginConfig;

import java.util.Arrays;
import java.util.List;
import java.util.ArrayList;

@JsonTypeName("syslog")
@JsonInclude(JsonInclude.Include.NON_DEFAULT)
public class SyslogFormatConfig implements FormatPluginConfig {

  public List<String> extensions;
  public int maxErrors = 10;
  public boolean flattenStructuredData;

  public boolean getFlattenStructuredData() {
    return flattenStructuredData;
  }

  public int getMaxErrors() {
    return maxErrors;
  }

  public List<String> getExtensions() {
    return extensions;
  }

  public void setExtensions(List ext) {
    this.extensions = ext;
  }

  public void setExtension(String ext) {
    if (this.extensions == null) {
      this.extensions = new ArrayList<String>();
    }
    this.extensions.add(ext);
  }

  public void setMaxErrors(int errors) {
    this.maxErrors = errors;
  }

  public void setFlattenStructuredData(boolean flattenData) {
    this.flattenStructuredData = flattenData;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null || getClass() != obj.getClass()) {
      return false;
    }
    SyslogFormatConfig other = (SyslogFormatConfig) obj;
    return Objects.equal(extensions, other.extensions) &&
            Objects.equal(maxErrors, other.maxErrors) &&
            Objects.equal(flattenStructuredData, other.flattenStructuredData);
  }

  @Override
  public int hashCode() {
    return Arrays.hashCode(new Object[]{maxErrors, flattenStructuredData, extensions});
  }
}
