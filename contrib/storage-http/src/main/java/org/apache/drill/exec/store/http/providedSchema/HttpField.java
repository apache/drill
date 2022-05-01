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


import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonPOJOBuilder;
import org.apache.drill.common.PlanStringBuilder;
import org.apache.drill.common.exceptions.UserException;
import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.exec.store.log.LogFormatField;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;

@JsonInclude(JsonInclude.Include.NON_DEFAULT)
@JsonDeserialize(builder = HttpField.HttpFieldBuilder.class)
public class HttpField {

  private static final Logger logger = LoggerFactory.getLogger(HttpField.class);

  @JsonProperty
  private final String fieldName;

  @JsonProperty
  private final String fieldType;

  @JsonProperty
  private final List<HttpField> fields;

  @JsonProperty
  private final boolean isArray;

  @JsonProperty
  private final Map<String, String> properties;

  public HttpField(HttpFieldBuilder builder) {
    this.fieldName = builder.fieldName;
    this.fieldType = builder.fieldType;
    this.fields = builder.fields;
    this.isArray = builder.isArray;
    this.properties = builder.properties;
  }

  public static HttpFieldBuilder builder() {
    return new HttpFieldBuilder();
  }

  @JsonProperty("fieldName")
  public String getFieldName() { return fieldName; }

  @JsonProperty("fieldType")
  public String getFieldType() { return fieldType; }

  @JsonProperty("fields")
  public List<HttpField> getFields() {
    return fields;
  }

  @JsonProperty("properties")
  public Map<String, String> getProperties() {
    if (properties == null) {
      return Collections.emptyMap();
    }
    return properties;
  }

  @JsonProperty("isArray")
  public boolean isArray() {
    return isArray;
  }

  @JsonIgnore
  public boolean isComplex() {
    return getDrillType() == MinorType.MAP ||
      getDrillType() == MinorType.DICT ||
      getDrillType() == MinorType.UNION;
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
      case "list":
        return MinorType.LIST;
      default:
        throw UserException.validationError()
          .message(fieldType + " is not a supported field type for JSON provided schemas.")
          .build(logger);
    }
  }

  @Override
  public boolean equals(Object o) {
    if (o == null || ! (o instanceof LogFormatField)) {
      return false;
    }
    HttpField other = (HttpField) o;
    return fieldName.equals(other.fieldName) &&
      Objects.equals(fieldType, other.fieldType) &&
      Objects.equals(fields, other.fields) &&
      Objects.equals(properties, other.properties) &&
      Objects.equals(isArray, other.isArray);
  }

  @Override
  public int hashCode() {
    return Objects.hash(fieldName, fieldType, fields, properties, isArray);
  }

  @Override
  public String toString() {
    return new PlanStringBuilder(this)
      .field("fieldName", fieldName)
      .field("fieldType", fieldType)
      .field("fields", fields)
      .field("properties", properties)
      .field("isArray", isArray)
      .toString();
  }

  @JsonPOJOBuilder(withPrefix = "")
  public static class HttpFieldBuilder {
    private String fieldName;
    private String fieldType;
    private List<HttpField> fields;
    private boolean isArray;
    private Map<String, String> properties;

    public HttpFieldBuilder fieldName(String fieldName) {
      this.fieldName = fieldName;
      return this;
    }

    public HttpFieldBuilder fieldType(String fieldType) {
      this.fieldType = fieldType;
      return this;
    }

    public HttpFieldBuilder fields(List<HttpField> fields) {
      this.fields = fields;
      return this;
    }

    public HttpFieldBuilder isArray(Boolean isArray) {
      this.isArray = isArray;
      return this;
    }

    public HttpFieldBuilder properties(Map<String, String> properties) {
      this.properties = properties;
      return this;
    }

    public HttpField build() {
      return new HttpField(this);
    }
  }
}
