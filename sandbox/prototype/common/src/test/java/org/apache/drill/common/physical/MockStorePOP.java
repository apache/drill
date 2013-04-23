/*******************************************************************************
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
 ******************************************************************************/
package org.apache.drill.common.physical;

import java.util.List;

import org.apache.drill.common.defs.PartitionDef;
import org.apache.drill.common.physical.MockStorePOP.MockWriteEntry;
import org.apache.drill.common.physical.pop.StorePOP;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;

@JsonTypeName("mock-store")
public class MockStorePOP extends StorePOP<MockWriteEntry>{
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(MockStorePOP.class);

  private List<String> fieldNames;

  
  @JsonCreator
  public MockStorePOP(@JsonProperty("output") FieldSet fields, @JsonProperty("mode") StoreMode mode, @JsonProperty("entries") List<MockWriteEntry> entries, @JsonProperty("partition") PartitionDef partition, @JsonProperty("fieldNames") List<String> fieldNames) {
    super(fields, mode, partition, entries);
    this.fieldNames = fieldNames;
  }

  
  public List<String> getFieldNames() {
    return fieldNames;
  }

  
  public static class MockWriteEntry implements WriteEntry{
    public String path;
    public String key;
    public String type;
  }
  
}

