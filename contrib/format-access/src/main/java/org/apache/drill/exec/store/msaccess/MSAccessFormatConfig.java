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

package org.apache.drill.exec.store.msaccess;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import org.apache.drill.common.PlanStringBuilder;
import org.apache.drill.common.logical.FormatPluginConfig;
import org.apache.drill.shaded.guava.com.google.common.collect.ImmutableList;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;

@JsonTypeName(MSAccessFormatPlugin.DEFAULT_NAME)
@JsonInclude(JsonInclude.Include.NON_DEFAULT)
public class MSAccessFormatConfig implements FormatPluginConfig {
  private final List<String> extensions;
  private final String tableName;

  // Omitted properties take reasonable defaults
  @JsonCreator
  public MSAccessFormatConfig(@JsonProperty("extensions") List<String> extensions,
                              @JsonProperty("tableName") String tableName) {
    this.extensions = extensions == null ? Arrays.asList("accdb", "mdb") : ImmutableList.copyOf(extensions);
    this.tableName = tableName;
  }

  @JsonInclude(Include.NON_DEFAULT)
  public List<String> getExtensions() {
    return extensions;
  }

  @JsonInclude(Include.NON_DEFAULT)
  public String getTableName() {
    return tableName;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    MSAccessFormatConfig that = (MSAccessFormatConfig) o;
    return Objects.equals(extensions, that.extensions) &&
        Objects.equals(tableName, that.tableName);
  }

  @Override
  public int hashCode() {
    return Objects.hash(extensions, tableName);
  }

  @Override
  public String toString() {
    return new PlanStringBuilder(this)
        .field("extensions", extensions)
        .field("tableName", tableName)
        .toString();
  }
}
