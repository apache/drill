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

package org.apache.drill.exec.store.http.providedSchema;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonTypeName;
import org.apache.drill.common.PlanStringBuilder;
import org.apache.drill.common.exceptions.UserException;
import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.exec.store.log.LogFormatField;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Objects;

@JsonTypeName("HttpSchemaFieldDescription")
@JsonInclude(JsonInclude.Include.NON_DEFAULT)
public class HttpField {

  private static final Logger logger = LoggerFactory.getLogger(HttpField.class);
  private final String fieldName;
  private final String fieldType;
  private final String format;
  private final List<HttpField> fields;

  public HttpField(String fieldName, String fieldType) {
    this(fieldName, fieldType, null, Collections.EMPTY_LIST);
  }

  @JsonCreator
  public HttpField(String fieldName, String fieldType, String format, List<HttpField> fields) {
    this.fieldName = fieldName;
    this.fieldType = fieldType;
    this.format = format;
    this.fields = fields;
  }

  public String getFieldName() { return fieldName; }

  public String getFieldType() { return fieldType; }

  public List<HttpField> getFields() {
    return fields;
  }

  @JsonIgnore
  public MinorType getDrillType() {
    switch (fieldType.toLowerCase(Locale.ROOT)) {
      case "varchar":
        return MinorType.VARCHAR;
      case "double":
      case "float8":
      case "float4":
        return MinorType.FLOAT8;
      case "int":
      case "integer":
      case "bigint":
        return MinorType.BIGINT;
      case "vardecimal":
        return MinorType.VARDECIMAL;
      case "date":
        return MinorType.DATE;
      case "time":
        return MinorType.TIME;
      case "timestamp":
        return MinorType.TIMESTAMP;
      case "interval":
        return MinorType.INTERVAL;
      case "boolean":
      case "bit":
      case "bool":
        return MinorType.BIT;
      // Complex Fields
      case "map":
        return MinorType.MAP;
      case "union":
        return MinorType.UNION;
      case "list":
        return MinorType.LIST;
      default:
        throw UserException.validationError()
          .message(fieldType + " is not a supported field type for JSON provided schemas.")
          .build(logger);
    }
  }

  public String getFormat() { return format; }

  @Override
  public boolean equals(Object o) {
    if (o == null || ! (o instanceof LogFormatField)) {
      return false;
    }
    HttpField other = (HttpField) o;
    return fieldName.equals(other.fieldName) &&
      Objects.equals(fieldType, other.fieldType) &&
      Objects.equals(format, other.format) &&
      Objects.equals(fields, other.fields);
  }

  @Override
  public int hashCode() {
    return Objects.hash(fieldName, fieldType, format, fields);
  }

  @Override
  public String toString() {
    return new PlanStringBuilder(this)
      .field("fieldName", fieldName)
      .field("fieldType", fieldType)
      .field("format", format)
      .toString();
  }
}
