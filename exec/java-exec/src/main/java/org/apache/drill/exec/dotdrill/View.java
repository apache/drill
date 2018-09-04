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
package org.apache.drill.exec.dotdrill;

import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.calcite.avatica.util.TimeUnit;
import org.apache.calcite.sql.SqlIntervalQualifier;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlTypeFamily;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.exec.planner.StarColumnHelper;
import org.apache.drill.exec.planner.types.RelDataTypeDrillImpl;
import org.apache.drill.exec.planner.types.RelDataTypeHolder;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.sql.type.SqlTypeName;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import org.apache.drill.shaded.guava.com.google.common.collect.Lists;

@JsonTypeName("view")
public class View {

  private final String name;
  private String sql;
  private List<FieldType> fields;

  /* Current schema when view is created (not the schema to which view belongs to) */
  private List<String> workspaceSchemaPath;

  @JsonInclude(Include.NON_NULL)
  public static class FieldType {

    private final String name;
    private final SqlTypeName type;
    private final Integer precision;
    private final Integer scale;
    private SqlIntervalQualifier intervalQualifier;
    private final Boolean isNullable;


    @JsonCreator
    public FieldType(
        @JsonProperty("name")                       String name,
        @JsonProperty("type")                       SqlTypeName type,
        @JsonProperty("precision")                  Integer precision,
        @JsonProperty("scale")                      Integer scale,
        @JsonProperty("startUnit")                  TimeUnit startUnit,
        @JsonProperty("endUnit")                    TimeUnit endUnit,
        @JsonProperty("fractionalSecondPrecision")  Integer fractionalSecondPrecision,
        @JsonProperty("isNullable")                 Boolean isNullable) {
      // Fix for views which were created on Calcite 1.4.
      // After Calcite upgrade star "*" was changed on dynamic star "**" (SchemaPath.DYNAMIC_STAR)
      // and type of star was changed to SqlTypeName.DYNAMIC_STAR
      this.name = "*".equals(name) ? SchemaPath.DYNAMIC_STAR : name;
      this.type = "*".equals(name) && type == SqlTypeName.ANY ? SqlTypeName.DYNAMIC_STAR : type;
      this.precision = precision;
      this.scale = scale;
      this.intervalQualifier =
          null == startUnit
          ? null
          : new SqlIntervalQualifier(
              startUnit, precision, endUnit, fractionalSecondPrecision, SqlParserPos.ZERO );

      // Property "isNullable" is not part of the initial view definition and
      // was added in DRILL-2342.  If the default value is null, consider it as
      // "true".  It is safe to default to "nullable" than "required" type.
      this.isNullable = isNullable == null ? true : isNullable;
    }

    public FieldType(String name, RelDataType dataType) {
      this.name = name;
      this.type = dataType.getSqlTypeName();

      Integer p = null;
      Integer s = null;
      Integer fractionalSecondPrecision = null;

      switch (dataType.getSqlTypeName()) {
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
      case INTERVAL_YEAR:
      case INTERVAL_YEAR_MONTH:
      case INTERVAL_MONTH:
      case INTERVAL_DAY:
      case INTERVAL_DAY_HOUR:
      case INTERVAL_DAY_MINUTE:
      case INTERVAL_DAY_SECOND:
      case INTERVAL_HOUR:
      case INTERVAL_HOUR_MINUTE:
      case INTERVAL_HOUR_SECOND:
      case INTERVAL_MINUTE:
      case INTERVAL_MINUTE_SECOND:
      case INTERVAL_SECOND:
        p = dataType.getIntervalQualifier().getStartPrecisionPreservingDefault();
      default:
        break;
      }

      this.precision = p;
      this.scale = s;
      this.intervalQualifier = dataType.getIntervalQualifier();
      this.isNullable = dataType.isNullable();
    }

    /**
     * Gets the name of this field.
     */
    public String getName() {
      return name;
    }

    /**
     * Gets the data type of this field.
     * (Data type only; not full datatype descriptor.)
     */
    public SqlTypeName getType() {
      return type;
    }

    /**
     * Gets the precision of the data type descriptor of this field.
     * The precision is the precision for a numeric type, the length for a
     * string type, or the start unit precision for an interval type.
     * */
    public Integer getPrecision() {
      return precision;
    }

    /**
     * Gets the numeric scale of the data type descriptor of this field,
     * for numeric types.
     */
    public Integer getScale() {
      return scale;
    }

    /**
     * Gets the interval type qualifier of the interval data type descriptor of
     * this field (<i>iff</i> interval type). */
    @JsonIgnore
    public SqlIntervalQualifier getIntervalQualifier() {
      return intervalQualifier;
    }

    /**
     * Gets the time range start unit of the type qualifier of the interval data
     * type descriptor of this field (<i>iff</i> interval type).
     */
    public TimeUnit getStartUnit() {
      return null == intervalQualifier ? null : intervalQualifier.getStartUnit();
    }

    /**
     * Gets the time range end unit of the type qualifier of the interval data
     * type descriptor of this field (<i>iff</i> interval type).
     */
    public TimeUnit getEndUnit() {
      return null == intervalQualifier ? null : intervalQualifier.getEndUnit();
    }

    /**
     * Gets the fractional second precision of the type qualifier of the interval
     * data type descriptor of this field (<i>iff</i> interval type).
     * Gets the interval type descriptor's fractional second precision
     * (<i>iff</i> interval type).
     */
    public Integer getFractionalSecondPrecision() {
      return null == intervalQualifier ? null : intervalQualifier.getFractionalSecondPrecisionPreservingDefault();
    }

    /**
     * Gets the nullability of the data type desription of this field.
     */
    public Boolean getIsNullable() {
      return isNullable;
    }

  }


  public View(String name, String sql, RelDataType rowType, List<String> workspaceSchemaPath) {
    this(name,
        sql,
        rowType.getFieldList().stream()
            .map(f -> new FieldType(f.getName(), f.getType()))
            .collect(Collectors.toList()),
        workspaceSchemaPath);
  }

  @JsonCreator
  public View(@JsonProperty("name") String name,
              @JsonProperty("sql") String sql,
              @JsonProperty("fields") List<FieldType> fields,
              @JsonProperty("workspaceSchemaPath") List<String> workspaceSchemaPath) {
    this.name = name;
    this.sql = sql;
    this.fields = fields;
    // for backward compatibility since now all schemas and workspaces are case insensitive and stored in lower case
    // make sure that given workspace schema path is also in lower case
    this.workspaceSchemaPath = workspaceSchemaPath == null ? Collections.EMPTY_LIST :
        workspaceSchemaPath.stream()
            .map(String::toLowerCase)
            .collect(Collectors.toList());
  }

  public RelDataType getRowType(RelDataTypeFactory factory) {

    // if there are no fields defined, this is a dynamic view.
    if (isDynamic()) {
      return new RelDataTypeDrillImpl(new RelDataTypeHolder(), factory);
    }

    List<RelDataType> types = Lists.newArrayList();
    List<String> names = Lists.newArrayList();

    for (FieldType field : fields) {
      names.add(field.getName());
      RelDataType type;
      if (   SqlTypeFamily.INTERVAL_YEAR_MONTH == field.getType().getFamily()
          || SqlTypeFamily.INTERVAL_DAY_TIME   == field.getType().getFamily() ) {
       type = factory.createSqlIntervalType( field.getIntervalQualifier() );
      } else if (field.getPrecision() == null && field.getScale() == null) {
        type = factory.createSqlType(field.getType());
      } else if (field.getPrecision() != null && field.getScale() == null) {
        type = factory.createSqlType(field.getType(), field.getPrecision());
      } else {
        type = factory.createSqlType(field.getType(), field.getPrecision(), field.getScale());
      }

      if (field.getIsNullable()) {
        types.add(factory.createTypeWithNullability(type, true));
      } else {
        types.add(type);
      }
    }
    return factory.createStructType(types, names);
  }

  @JsonIgnore
  public boolean isDynamic(){
    return fields.isEmpty();
  }

  @JsonIgnore
  public boolean hasStar() {
    for (FieldType field : fields) {
      if (StarColumnHelper.isNonPrefixedStarColumn(field.getName())) {
        return true;
      }
    }
    return false;
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
