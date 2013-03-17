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
package org.apache.drill.common.physical.pop;

import java.util.List;

import org.apache.drill.common.JSONOptions;
import org.apache.drill.common.defs.PartitionDef;
import org.apache.drill.common.physical.FieldSet;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;

@JsonTypeName("store")
public class StorePOP extends SingleChildPOP implements SinkPOP{
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(StorePOP.class);

  public static enum StoreMode {SYSTEM_CHOICE, PREDEFINED_PARTITIONS};
  
  private StoreMode mode;
  private PartitionDef partitioning;
  
  @JsonCreator
  public StorePOP(@JsonProperty("storageengine") String storageEngineName, @JsonProperty("fields") FieldSet fieldSet, @JsonProperty("mode") StoreMode mode, @JsonProperty("entries") List<JSONOptions> entries) {
    super(fieldSet);
  }

  public StoreMode getMode() {
    return mode;
  }

  public PartitionDef getPartitioning() {
    return partitioning;
  }

  
  
}
