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

package org.apache.drill.exec.store.daffodil;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.google.common.collect.ImmutableList;
import org.apache.drill.common.PlanStringBuilder;
import org.apache.drill.common.logical.FormatPluginConfig;
import org.apache.drill.exec.store.daffodil.DaffodilBatchReader.DaffodilReaderConfig;

import java.util.Collections;
import java.util.List;
import java.util.Objects;

@JsonTypeName(DaffodilFormatPlugin.DEFAULT_NAME)
@JsonInclude(JsonInclude.Include.NON_DEFAULT)
public class DaffodilFormatConfig implements FormatPluginConfig {

  public final List<String> extensions;
  public final String schemaURI;
  public final boolean validationMode;
  public final String rootName;
  public final String rootNamespace;
  /**
   * In the constructor for a format config, you should not use
   * boxed versions of primitive types. It creates problems with
   * defaulting them (they default to null) which cannot be unboxed.
   */
  @JsonCreator
  public DaffodilFormatConfig(@JsonProperty("extensions") List<String> extensions,
                        @JsonProperty("schemaURI") String schemaURI,
                        @JsonProperty("rootName") String rootName,
                        @JsonProperty("rootNamespace") String rootNamespace,
                        @JsonProperty("validationMode") boolean validationMode) {

    this.extensions = extensions == null ? Collections.singletonList("dat") : ImmutableList.copyOf(extensions);
    this.rootName = rootName;
    this.rootNamespace = rootNamespace;
    this.schemaURI = schemaURI;
    // no default. Users must pick.
    this.validationMode = validationMode;
  }

  @JsonInclude(JsonInclude.Include.NON_DEFAULT)
  public List<String> getExtensions() {
    return extensions;
  }

  public String getSchemaURI() {
    return schemaURI;
  }

  public String getRootName() {
    return rootName;
  }

  public String getRootNamespace() {
    return rootNamespace;
  }

  @JsonInclude(JsonInclude.Include.NON_DEFAULT)
  public boolean getValidationMode() {
    return validationMode;
  }

  public DaffodilReaderConfig getReaderConfig(DaffodilFormatPlugin plugin) {
    DaffodilReaderConfig readerConfig = new DaffodilReaderConfig(plugin);
    return readerConfig;
  }

  @Override
  public int hashCode() {
    return Objects.hash(schemaURI, validationMode, rootName, rootNamespace);
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null || getClass() != obj.getClass()) {
      return false;
    }
    DaffodilFormatConfig other = (DaffodilFormatConfig) obj;
    return Objects.equals(schemaURI, other.schemaURI)
        && Objects.equals(rootName, other.rootName)
        && Objects.equals(rootNamespace, other.rootNamespace)
        && Objects.equals(validationMode, other.validationMode);
  }

  @Override
  public String toString() {
    return new PlanStringBuilder(this)
        .field("schemaURI", schemaURI)
        .field("rootName", rootName)
        .field("rootNamespace", rootNamespace)
        .field("validationMode", validationMode)
      .toString();
  }
}
