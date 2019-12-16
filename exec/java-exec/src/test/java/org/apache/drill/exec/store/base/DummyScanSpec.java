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
package org.apache.drill.exec.store.base;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;

@JsonTypeName("dummy-scan-spec")
@JsonInclude(Include.NON_NULL)
public class DummyScanSpec  {

  protected final String schemaName;
  protected final String tableName;

  @JsonCreator
  public DummyScanSpec(
      @JsonProperty("schemaName") String schemaName,
      @JsonProperty("tableName") String tableName) {
    this.schemaName = schemaName;
    this.tableName = tableName;
  }

  @JsonProperty("schemaName")
  public String schemaName() { return schemaName; }

  @JsonProperty("tableName")
  public String tableName() { return tableName; }

  @Override
  public String toString() {
    PlanStringBuilder builder = new PlanStringBuilder(this);
    if (schemaName != null && !schemaName.equals(BaseStoragePlugin.DEFAULT_SCHEMA_NAME)) {
      builder.field("schema", schemaName);
    }
    builder.field("table", tableName);
    return builder.toString();
  }
}
