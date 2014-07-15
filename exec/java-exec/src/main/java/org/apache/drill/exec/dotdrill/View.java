/**
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
package org.apache.drill.exec.dotdrill;

import java.util.List;

import com.google.common.collect.ImmutableList;
import org.apache.drill.exec.planner.types.RelDataTypeDrillImpl;
import org.apache.drill.exec.planner.types.RelDataTypeHolder;
import org.eigenbase.reltype.RelDataType;
import org.eigenbase.reltype.RelDataTypeFactory;
import org.eigenbase.reltype.RelDataTypeField;
import org.eigenbase.sql.type.SqlTypeName;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.google.common.collect.Lists;

@JsonTypeName("view")
public class View {

  private final String name;
  private String sql;
  private List<FieldType> fields;

  /* Current schema when view is created (not the schema to which view belongs to) */
  private List<String> workspaceSchemaPath;

  @JsonInclude(Include.NON_NULL)
  public static class FieldType {
    public final String name;
    public final SqlTypeName type;
    public final Integer precision;
    public final Integer scale;

    @JsonCreator
    public FieldType(@JsonProperty("name") String name, @JsonProperty("type") SqlTypeName type, @JsonProperty("precision") Integer precision, @JsonProperty("scale") Integer scale){
      this.name = name;
      this.type = type;
      this.precision = precision;
      this.scale = scale;
    }

    public FieldType(String name, RelDataType dataType){
      this.name = name;
      this.type = dataType.getSqlTypeName();
      Integer p = null;
      Integer s = null;

      switch(dataType.getSqlTypeName()){
      case CHAR:
      case BINARY:
      case VARBINARY:
      case VARCHAR:
        p = dataType.getPrecision();
        break;
      case DECIMAL:
        p = dataType.getPrecision();
        s = dataType.getScale();
        break;
      }

      this.precision = p;
      this.scale = s;
    }
  }

  public View(String name, String sql, RelDataType rowType, List<String> workspaceSchemaPath){
    this.name = name;
    this.sql = sql;
    fields = Lists.newArrayList();
    for(RelDataTypeField f : rowType.getFieldList()){
      fields.add(new FieldType(f.getName(), f.getType()));
    }
    this.workspaceSchemaPath =
        workspaceSchemaPath == null ? ImmutableList.<String>of() : ImmutableList.copyOf(workspaceSchemaPath);
  }

  @JsonCreator
  public View(@JsonProperty("name") String name,
              @JsonProperty("sql") String sql,
              @JsonProperty("fields") List<FieldType> fields,
              @JsonProperty("workspaceSchemaPath") List<String> workspaceSchemaPath){
    this.name = name;
    this.sql = sql;
    this.fields = fields;
    this.workspaceSchemaPath =
        workspaceSchemaPath == null ? ImmutableList.<String>of() : ImmutableList.copyOf(workspaceSchemaPath);
  }

  public RelDataType getRowType(RelDataTypeFactory factory){

    // if there are no fields defined, this is a dynamic view.
    if(isDynamic()){
      return new RelDataTypeDrillImpl(new RelDataTypeHolder(), factory);
    }

    List<RelDataType> types = Lists.newArrayList();
    List<String> names = Lists.newArrayList();

    for(FieldType field : fields){
      names.add(field.name);
      if(field.precision == null && field.scale == null){
        types.add(factory.createSqlType(field.type));
      }else if(field.precision != null){
        types.add(factory.createSqlType(field.type, field.precision));
      }else{
        types.add(factory.createSqlType(field.type, field.precision, field.scale));
      }
    }
    return factory.createStructType(types, names);
  }

  @JsonIgnore
  public boolean isDynamic(){
    return fields.isEmpty();
  }

  public String getSql() {
    return sql;
  }

  public void setSql(String sql) {
    this.sql = sql;
  }

  public String getName() {
    return name;
  }

  public List<FieldType> getFields() {
    return fields;
  }

  public List<String> getWorkspaceSchemaPath() {
    return workspaceSchemaPath;
  }



}
