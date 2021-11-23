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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import org.apache.drill.common.PlanStringBuilder;
import org.apache.drill.common.exceptions.UserException;
import org.apache.drill.common.logical.FormatPluginConfig;
import org.apache.drill.common.types.TypeProtos;
import org.apache.drill.shaded.guava.com.google.common.collect.ImmutableList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

@JsonTypeName(FixedwidthFormatPlugin.DEFAULT_NAME)
@JsonInclude(JsonInclude.Include.NON_DEFAULT)
public class FixedwidthFormatConfig implements FormatPluginConfig {
  private static final Logger logger = LoggerFactory.getLogger(FixedwidthFormatConfig.class);
  private final List<String> extensions;
  private final List<FixedwidthFieldConfig> fields;
  private final List<TypeProtos.MinorType> validDataTypes = Arrays.asList(new TypeProtos.MinorType[]{TypeProtos.MinorType.INT, TypeProtos.MinorType.VARCHAR,
    TypeProtos.MinorType.DATE, TypeProtos.MinorType.TIME, TypeProtos.MinorType.TIMESTAMP, TypeProtos.MinorType.FLOAT4,
  TypeProtos.MinorType.FLOAT8, TypeProtos.MinorType.BIGINT, TypeProtos.MinorType.VARDECIMAL});

  @JsonCreator
  public FixedwidthFormatConfig(@JsonProperty("extensions") List<String> extensions,
                                @JsonProperty("fields") List<FixedwidthFieldConfig> fields) {
    this.extensions = extensions == null ? Collections.singletonList("fwf") : ImmutableList.copyOf(extensions);
    Collections.sort(fields);
    this.fields = fields;

    validateFieldInput();
  }

  @JsonInclude(JsonInclude.Include.NON_DEFAULT)
  public List<String> getExtensions() {
    return extensions;
  }

  public List<FixedwidthFieldConfig> getFields() {
    return fields;
  }

  @Override
  public int hashCode() {
    return Objects.hash(extensions, fields);
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null || getClass() != obj.getClass()) {
      return false;
    }
    FixedwidthFormatConfig other = (FixedwidthFormatConfig) obj;
    return Objects.equals(extensions, other.extensions)
            && Objects.equals(fields, other.fields);
  }

  @Override
  public String toString() {
    return new PlanStringBuilder(this)
            .field("extensions", extensions)
            .field("fields", fields)
            .toString();
  }


  @JsonIgnore
  public boolean hasFields() {
    return fields != null && ! fields.isEmpty();
  }

  @JsonIgnore
  public List<String> getFieldNames() {
    List<String> result = new ArrayList<>();
    if (! hasFields()) {
      return result;
    }

    for (FixedwidthFieldConfig field : fields) {
      result.add(field.getName());
    }
    return result;
  }

  @JsonIgnore
  public List<Integer> getFieldIndices() {
    List<Integer> result = new ArrayList<>();
    if (! hasFields()) {
      return result;
    }

    for (FixedwidthFieldConfig field : fields) {
      result.add(field.getIndex());
    }
    return result;
  }

  @JsonIgnore
  public List<Integer> getFieldWidths() {
    List<Integer> result = new ArrayList<>();
    if (! hasFields()) {
      return result;
    }

    for (FixedwidthFieldConfig field : fields) {
      result.add(field.getWidth());
    }
    return result;
  }

  @JsonIgnore
  public void setFieldWidths(int i, int value) {
    for (FixedwidthFieldConfig field : fields) {
      if (field.getIndex() == i) {
        field.setWidth(value);
      }
    }
  }

  @JsonIgnore
  public List<TypeProtos.MinorType> getFieldTypes() {
    List<TypeProtos.MinorType> result = new ArrayList<>();
    if (! hasFields()) {
      return result;
    }

    for (FixedwidthFieldConfig field : fields) {
      result.add(field.getType());
    }
    return result;
  }

  @JsonIgnore
  public void setFieldTypes(int i) {
    for (FixedwidthFieldConfig field : fields) {
      if (field.getIndex() == i) {
        field.setType();
      }
    }
  }

  @JsonIgnore
  public void validateFieldInput(){
    Set<String> uniqueNames = new HashSet<>();
    List<Integer> fieldIndices = this.getFieldIndices();
    List<Integer> fieldWidths = this.getFieldWidths();
    List<String> fieldNames = this.getFieldNames();
    List<TypeProtos.MinorType> fieldTypes = this.getFieldTypes();
    int width = 0;
    int prevIndexAndWidth = -1;

    // Ensure no two fields have the same name
    for (String name : this.getFieldNames()){
      if (name.length() == 0){
        throw UserException
          .validationError()
          .message("Blank field name detected.")
          .addContext("Plugin", FixedwidthFormatPlugin.DEFAULT_NAME)
          .build(logger);
      }
      if (uniqueNames.contains(name)){
        throw UserException
          .validationError()
          .message("Duplicate column name: " + name)
          .addContext("Plugin", FixedwidthFormatPlugin.DEFAULT_NAME)
          .build(logger);
      }
      uniqueNames.add(name);
    }

    //assuming that fieldIndices is the same size as fieldWidths, width is required
    for (int i = 0; i<fieldIndices.size(); i++) {
      if (fieldIndices.get(i) < 1) {
        throw UserException
          .validationError()
          .message("Invalid index for field '" + fieldNames.get(i) + "' at index: " + fieldIndices.get(i) + ". Index must be > 0.")
          .addContext("Plugin", FixedwidthFormatPlugin.DEFAULT_NAME)
          .build(logger);
      }
      else if (fieldIndices.get(i) <= prevIndexAndWidth) {
        throw UserException
          .validationError()
          .message("Overlapping fields: " + fieldNames.get(i-1) + " and " + fieldNames.get(i))
          .addContext("Plugin", FixedwidthFormatPlugin.DEFAULT_NAME)
          .build(logger);
      }

      if (fieldWidths.get(i) == null || fieldWidths.get(i) < 1) {
        // Come back to this - can we calculate this instead of throwing an error?
        if (i == fieldIndices.size()-1) {
          throw UserException
            .validationError()
            .message("Width for field '" + fieldNames.get(i) + "' is empty.")
            .addContext("Plugin", FixedwidthFormatPlugin.DEFAULT_NAME)
            .build(logger);
        }
        width = fieldIndices.get(i+1) - fieldIndices.get(i) - 1;
        setFieldWidths(fieldIndices.get(i), width);
      }
        prevIndexAndWidth = fieldIndices.get(i) + fieldWidths.get(i);

      // Validate Field Type
      if (fieldTypes.get(i) == null || fieldTypes.get(i).toString().length() == 0) {
        setFieldTypes(fieldIndices.get(i));
      }
      else if (!validDataTypes.contains(fieldTypes.get(i))){
        setFieldTypes(fieldIndices.get(i)); //Should we throw an error or default to VARCHAR for data types that are not yet available in this plugin
      }
    }
  }
}
