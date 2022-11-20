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
package org.apache.drill.exec.store.drill.plugin;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.drill.common.PlanStringBuilder;
import org.apache.drill.exec.planner.logical.DrillTableSelection;

import java.util.List;

public class DrillScanSpec implements DrillTableSelection {
  private List<String> schemaPath;
  private String collectionName;

  private String query;

  @JsonCreator
  public DrillScanSpec(@JsonProperty("schemaPath") List<String> schemaPath,
      @JsonProperty("collectionName") String collectionName) {
    this.schemaPath = schemaPath;
    this.collectionName = collectionName;
  }

  public DrillScanSpec(String query) {
    this.query = query;
  }

  public List<String> getSchemaPath() {
    return this.schemaPath;
  }

  public String getCollectionName() {
    return this.collectionName;
  }

  public String getQuery() {
    return this.query;
  }

  @Override
  public String toString() {
    return new PlanStringBuilder(this)
      .field("schemaPath", schemaPath)
      .field("collectionName", collectionName)
      .field("query", query)
      .toString();
  }

  @Override
  public String digest() {
    return toString();
  }
}
