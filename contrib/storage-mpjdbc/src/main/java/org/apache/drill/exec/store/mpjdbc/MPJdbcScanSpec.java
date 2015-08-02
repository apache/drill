/**
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
package org.apache.drill.exec.store.mpjdbc;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;

public class MPJdbcScanSpec {

  private String database;
  private String table;
  private String columns;

  @JsonIgnore
  private List<String> filters;

  @JsonCreator
  public MPJdbcScanSpec(@JsonProperty("database") String database,
      @JsonProperty("table") String table, @JsonProperty("columns") String columns) {
    this.database = database;
    this.table = table;
    this.columns = columns;
  }

  public MPJdbcScanSpec(String database, String table, List<String> filters, String columns) {
    this.database = database;
    this.table = table;
    this.filters = filters;
    this.columns = columns;
  }

  public String getDatabase() {
    return this.database;
  }

  public String getTable() {
    return this.table;
  }

  public List<String> getFilters() {
    return this.filters;
  }

  public String getColumns() {
    return this.columns;
  }
  @Override
  public String toString() {
    return "MPJdbcScanSpec [Database=" + database + ", table=" + table
        + ", columns=" + columns + ", filters=" + filters + "]";
  }

  @Override
  public boolean equals(Object obj) {
    // TODO Auto-generated method stub
    return super.equals(obj);
  }
}
