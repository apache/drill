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

package org.apache.drill.exec.store.hdf5;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonTypeName;
import org.apache.drill.common.logical.FormatPluginConfig;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

@JsonTypeName(HDF5FormatPlugin.DEFAULT_NAME)
public class HDF5FormatConfig implements FormatPluginConfig {

  public List<String> extensions = Collections.singletonList("h5");

  public String defaultPath;

  @JsonInclude(JsonInclude.Include.NON_DEFAULT)
  public List<String> getExtensions() {
    return extensions;
  }

  public String getDefaultPath() {
    return defaultPath;
  }


  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null || getClass() != obj.getClass()) {
      return false;
    }
    HDF5FormatConfig other = (HDF5FormatConfig) obj;
    return Objects.equals(extensions, other.getExtensions()) &&
      Objects.equals(defaultPath, other.defaultPath);
  }

  @Override
  public int hashCode() {
    return Arrays.hashCode(new Object[]{extensions, defaultPath});
  }
}
