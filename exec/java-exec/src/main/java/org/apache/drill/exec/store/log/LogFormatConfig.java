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
package org.apache.drill.exec.store.log;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonTypeName;
import org.apache.drill.shaded.guava.com.google.common.base.Objects;
import org.apache.drill.common.logical.FormatPluginConfig;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

@JsonTypeName("logRegex")
public class LogFormatConfig implements FormatPluginConfig {

  private String regex;
  private String extension;
  private int maxErrors = 10;
  private List<LogFormatField> schema;

  public String getRegex() {
    return regex;
  }

  public String getExtension() {
    return extension;
  }

  public int getMaxErrors() {
    return maxErrors;
  }

  public List<LogFormatField> getSchema() {
    return schema;
  }

  //Setters
  public void setExtension(String ext) {
    this.extension = ext;
  }

  public void setMaxErrors(int errors) {
    this.maxErrors = errors;
  }

  public void setRegex(String regex) {
    this.regex = regex;
  }

  public void setSchema() {
    this.schema = new ArrayList<LogFormatField>();
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null || getClass() != obj.getClass()) {
      return false;
    }
    LogFormatConfig other = (LogFormatConfig) obj;
    return Objects.equal(regex, other.regex) &&
        Objects.equal(maxErrors, other.maxErrors) &&
        Objects.equal(schema, other.schema) &&
        Objects.equal(extension, other.extension);
  }

  @Override
  public int hashCode() {
    return Arrays.hashCode(new Object[]{regex, maxErrors, schema, extension});
  }

  @JsonIgnore
  public List<String> getFieldNames() {
    List<String> result = new ArrayList<String>();
    if (this.schema == null) {
      return result;
    }

    for (LogFormatField field : this.schema) {
      result.add(field.getFieldName());
    }
    return result;
  }

  @JsonIgnore
  public String getDataType(int fieldIndex) {
    LogFormatField f = this.schema.get(fieldIndex);
    return f.getFieldType().toUpperCase();
  }

  @JsonIgnore
  public LogFormatField getField(int fieldIndex) {
    return this.schema.get(fieldIndex);
  }

  @JsonIgnore
  public String getDateFormat(int patternIndex) {
    LogFormatField f = this.schema.get(patternIndex);
    return f.getFormat();
  }
}