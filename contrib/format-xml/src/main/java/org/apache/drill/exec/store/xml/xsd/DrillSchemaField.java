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

package org.apache.drill.exec.store.xml.xsd;

import org.apache.drill.common.PlanStringBuilder;
import org.apache.drill.common.types.TypeProtos.DataMode;
import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.exec.record.metadata.SchemaBuilder;
import org.apache.drill.shaded.guava.com.google.common.collect.ImmutableMap;

public class DrillSchemaField {

  protected String fieldName;
  protected MinorType type;
  protected DataMode mode;
  private MinorType DEFAULT_TYPE = MinorType.VARCHAR;

  private static final ImmutableMap<String, MinorType> XML_TYPE_MAPPINGS = ImmutableMap.<String, MinorType>builder()
    .put("BASE64BINARY", MinorType.VARBINARY)
    .put("BOOLEAN", MinorType.BIT)
    .put("DATE", MinorType.DATE)
    .put("DATETIME", MinorType.TIMESTAMP)
    .put("DECIMAL", MinorType.VARDECIMAL)
    .put("DOUBLE", MinorType.FLOAT8)
    .put("DURATION", MinorType.INTERVAL)
    .put("FLOAT", MinorType.FLOAT4)
    .put("HEXBINARY", MinorType.VARBINARY)
    .put("STRING", MinorType.VARCHAR)
    .put("TIME", MinorType.TIME)
    .build();

  @Override
  public String toString() {
    return new PlanStringBuilder(this)
      .field("fieldName", fieldName)
      .field("dataType", type)
      .field("mode", mode)
      .toString();
  }

  public MinorType getDrillDataType(String xmlType) {
    try {
      MinorType type = XML_TYPE_MAPPINGS.get(xmlType);
      if (type == null ) {
         return DEFAULT_TYPE;
       } else {
         return type;
       }
    } catch (NullPointerException e) {
      return DEFAULT_TYPE;
    }
  }

  public void addToDrillSchema(SchemaBuilder builder) {
    builder.add(fieldName, type, mode);
  }
}
