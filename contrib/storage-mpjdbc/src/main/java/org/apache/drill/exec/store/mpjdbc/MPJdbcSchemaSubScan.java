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
import org.apache.drill.exec.physical.base.AbstractSubScan;
import org.apache.drill.exec.store.mpjdbc.MPJdbcSchemaFilter;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
public class MPJdbcSchemaSubScan extends AbstractSubScan {
private final String table;
private final MPJdbcSchemaFilter filter;
private final String userName;

  @JsonCreator
  public MPJdbcSchemaSubScan(@JsonProperty("userName") String userName,
       @JsonProperty("table") String table,
       @JsonProperty("filter") MPJdbcSchemaFilter filter) {
    super(userName);
    this.table = table;
    this.filter = filter;
    this.userName = userName;
  }
  @JsonProperty("table")
  public String getTable() {
    return table;
  }
  @JsonProperty("filter")
  public MPJdbcSchemaFilter getFilter() {
    return filter;
  }
  @JsonProperty("userName")
  public String getUserName() {
    return this.userName;
  }
@Override
public int getOperatorType() {
// TODO Auto-generated method stub
return 0;
}
}