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
import org.apache.drill.shaded.guava.com.google.common.collect.ImmutableList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
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

  @JsonCreator
  public FixedwidthFormatConfig(@JsonProperty("extensions") List<String> extensions,
                                @JsonProperty("fields") List<FixedwidthFieldConfig> fields) {
    this.extensions = extensions == null ? Collections.singletonList("fwf") : ImmutableList.copyOf(extensions);
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
  public void validateFieldInput(){
    Set<String> uniqueNames = new HashSet<>();
    for (String name : this.getFieldNames()){
      /*if (name.length() == 0){

      }*/
      if (uniqueNames.contains(name)){
        throw UserException
          .validationError()
          .message("Duplicate column name: " + name)
          .addContext("Plugin", FixedwidthFormatPlugin.DEFAULT_NAME)
          .build(logger);
      }
      uniqueNames.add(name);
    }
    List<Integer> fieldIndices = this.getFieldIndices();
    List<Integer> fieldWidths = this.getFieldWidths();
    List<String> fieldNames = this.getFieldNames();
    int prevIndexAndWidth = -1;

    //assuming that fieldIndices is the same size as fieldWidths, width is required
    for (int i = 0; i<fieldIndices.size(); i++) {
      if (fieldIndices.get(i) < 1) {
        throw UserException
          .validationError()
          .message("Invalid index for field '" + fieldNames.get(i) + "' at index: " + fieldIndices.get(i) + ". Index must be > 0.")
          .addContext("Plugin", FixedwidthFormatPlugin.DEFAULT_NAME)
          .build(logger);
      }
      /*
      else if (fieldWidths.get(i) == null || fieldWidths.get(i) < 1) {
        if (i == fieldIndices.size()-1) {
          Integer width =
        }
        Integer width = fieldIndices.get(i+1) - fieldIndices.get(i);
        fieldWidths.set(i, width);
      }
       */
        else if (fieldIndices.get(i) <= prevIndexAndWidth) {
         throw UserException
           .validationError()
           .message("Overlapping fields: " + fieldNames.get(i-1) + " and " + fieldNames.get(i))
           .addContext("Plugin", FixedwidthFormatPlugin.DEFAULT_NAME)
           .build(logger);
       }
       prevIndexAndWidth = fieldIndices.get(i) + fieldWidths.get(i);
    }
  }

}
