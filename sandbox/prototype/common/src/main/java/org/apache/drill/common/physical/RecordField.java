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

import org.apache.drill.common.expression.types.DataType;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public class RecordField {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(RecordField.class);

  
  private String name;
  private DataType type;
  private ValueMode mode;
  
  @JsonCreator
  public RecordField(@JsonProperty("name") String name, @JsonProperty("type") DataType type, @JsonProperty("mode") ValueMode mode) {
    super();
    this.name = name;
    this.type = type;
    this.mode = mode;
  }

  public String getName() {
    return name;
  }

  public DataType getType() {
    return type;
  }

  public ValueMode getMode() {
    return mode;
  }
  
  public static enum ValueMode {
    VECTOR,
    DICT,
    RLE
  }
  
  public static enum ValueType {
    OPTIONAL,
    REQUIRED, 
    REPEATED
  }
  
  
}
