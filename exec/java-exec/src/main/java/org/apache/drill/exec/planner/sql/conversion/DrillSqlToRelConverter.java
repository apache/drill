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
package org.apache.drill.exec.planner.sql.conversion;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexDynamicParam;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.sql.SqlDynamicParam;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql2rel.SqlRexConvertletTable;
import org.apache.calcite.sql2rel.SqlToRelConverter;
import org.apache.calcite.util.NlsString;

/** Converts Drill SQL to relational expressions, preserving bound cache parameters. */
public final class DrillSqlToRelConverter extends SqlToRelConverter {
  public DrillSqlToRelConverter(RelOptTable.ViewExpander viewExpander, SqlValidator validator,
      DrillCalciteCatalogReader catalog, RelOptCluster cluster,
      SqlRexConvertletTable convertletTable, Config config) {
    super(viewExpander, validator, catalog, cluster, convertletTable, config);
  }

  @Override
  public RexDynamicParam convertDynamicParam(SqlDynamicParam param) {
    if (!(param instanceof SqlBoundDynamicParam)) {
      return super.convertDynamicParam(param);
    }
    // Register the parameter with Calcite before reading its inferred type.
    RexDynamicParam converted = super.convertDynamicParam(param);
    SqlLiteral source = ((SqlBoundDynamicParam) param).getLiteral();
    Object literalValue = source.getValue();
    if (literalValue instanceof NlsString) {
      literalValue = ((NlsString) literalValue).getValue();
    }
    // Calcite may leave a dynamic parameter as ANY. RexBuilder then guesses a
    // type from the Java value and can turn an exact decimal into BIGINT.
    // Preserve the SQL literal's own precision and scale in that case.
    RelDataType valueType = converted.getType().getSqlTypeName() == SqlTypeName.ANY
        ? source.createSqlType(rexBuilder.getTypeFactory()) : converted.getType();
    RexLiteral value = rexBuilder.makeLiteral(literalValue, valueType);
    return new RexBoundDynamicParam(converted.getType(), param.getIndex(), value);
  }
}
