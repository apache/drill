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
package org.apache.drill.exec.store.ltsv;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonTypeName;
import org.apache.drill.common.logical.FormatPluginConfig;
import org.apache.drill.shaded.guava.com.google.common.collect.ImmutableList;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;

@JsonTypeName("ltsv")
public class LTSVFormatPluginConfig implements FormatPluginConfig {
  private static final List<String> DEFAULT_EXTS = ImmutableList.of("ltsv");

  public List<String> extensions;

  @JsonInclude(JsonInclude.Include.NON_DEFAULT)
  public List<String> getExtensions() {
    if (extensions == null) {
      // when loading an old JSONFormatConfig that doesn't contain an "extensions" attribute
      return DEFAULT_EXTS;
    }
    return extensions;
  }

  @Override
  public int hashCode() {
    List<String> array = extensions != null ? extensions : DEFAULT_EXTS;
    return Arrays.hashCode(array.toArray(new String[array.size()]));
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    } else if (obj == null || getClass() != obj.getClass()) {
      return false;
    }
    LTSVFormatPluginConfig that = (LTSVFormatPluginConfig) obj;
    return Objects.equals(extensions, that.extensions);
  }
}
