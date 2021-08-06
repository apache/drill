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

package org.apache.drill.exec.store.fixedwidth;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import org.apache.drill.common.types.TypeProtos;

@JsonTypeName("fixedwidthReaderFieldDescription")
@JsonInclude(JsonInclude.Include.NON_DEFAULT)
public class FixedwidthFieldConfig {

  private final TypeProtos.MinorType dataType;
  private final String fieldName;
  private final String dateTimeFormat;
  private final int startIndex;
  private final int fieldWidth;

  public FixedwidthFieldConfig(@JsonProperty("dataType") TypeProtos.MinorType dataType,
                               @JsonProperty("fieldName") String fieldName,
                               @JsonProperty("dateTimeFormat") String dateTimeFormat,
                               @JsonProperty("startIndex") int startIndex,
                               @JsonProperty("fieldWidth") int fieldWidth) {
    this.dataType = dataType;
    this.fieldName = fieldName;
    this.dateTimeFormat = dateTimeFormat;
    this.startIndex = startIndex;
    this.fieldWidth = fieldWidth;
  }

  public TypeProtos.MinorType getDataType() {return dataType;}

  public String getFieldName() {return fieldName;}

  public String getDateTimeFormat() {return dateTimeFormat;}

  public int getStartIndex() {return startIndex;}

  public int getFieldWidth() {return fieldWidth;}
}
