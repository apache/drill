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
package org.apache.drill.exec.record.metadata.schema;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.drill.exec.record.metadata.ColumnMetadata;
import org.apache.drill.exec.record.metadata.TupleMetadata;
import org.apache.drill.exec.record.metadata.schema.parser.SchemaExprParser;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Holder class that contains table name, schema definition
 * and properties passed in schema file or using table function.
 */
@JsonInclude(JsonInclude.Include.NON_DEFAULT)
public class SchemaContainer {

  private final String table;
  private final TupleMetadata schema;
  // preserve properties order
  private final Map<String, String> properties = new LinkedHashMap<>();
  private final Version version;

  @JsonCreator
  public SchemaContainer(@JsonProperty("table") String table,
                         @JsonProperty("schema") List<String> schema,
                         @JsonProperty("properties") LinkedHashMap<String, String> properties,
                         @JsonProperty("version") Integer version) {
    this(table, schema == null ? null : String.join(", ", schema), properties, version);
  }

  public SchemaContainer(String table, String schema, Map<String, String> properties) {
    this(table, schema, properties, Version.VERSION_1); //current default version
  }

  public SchemaContainer(String table, String schema, Map<String, String> properties, Integer version) {
    this.table = table;
    this.schema = schema == null ? null : convert(schema);
    if (properties != null) {
      this.properties.putAll(properties);
    }
    this.version = new Version(version);
  }

  @JsonProperty("table")
  public String getTable() {
    return table;
  }

  @JsonProperty("schema")
  public List<String> getSchemaList() {
    return schema == null ? null : schema.toMetadataList().stream()
      .map(ColumnMetadata::columnString)
      .collect(Collectors.toList());
  }

  @JsonProperty("properties")
  public Map<String, String> getProperties() {
    return properties;
  }

  @JsonProperty("version")
  public Integer getVersionValue() {
    return version.getValue();
  }

  @JsonIgnore
  public TupleMetadata getSchema() {
    return schema;
  }

  @JsonIgnore
  public Version getVersion() {
    return version;
  }

  private TupleMetadata convert(String schema) {
    return SchemaExprParser.parseSchema(schema);
  }

  @Override
  public String toString() {
    return "SchemaContainer{" + "table='" + table + '\'' + ", schema=" + schema +
      ", properties=" + properties + ", version=" + version + '}';
  }

  /**
   * Schema container version holder contains version in int representation.
   * If during initialization null or less then 1 was given, replaces it with
   * {@link #UNDEFINED_VERSION} value.
   */
  public static class Version {

    public static final int UNDEFINED_VERSION = -1;
    public static final int VERSION_1 = 1;

    // is used for testing
    public static final int CURRENT_DEFAULT_VERSION = VERSION_1;

    private final int value;

    public Version(Integer value) {
      this.value = value == null || value < 1 ? UNDEFINED_VERSION : value;
    }

    public int getValue() {
      return value;
    }

    public boolean isUndefined() {
      return UNDEFINED_VERSION == value;
    }

    public int compare(Version versionToCompare) {
      return Integer.compare(value, versionToCompare.value);
    }

    @Override
    public String toString() {
      return "Version{" + "value=" + value + '}';
    }
  }
}
