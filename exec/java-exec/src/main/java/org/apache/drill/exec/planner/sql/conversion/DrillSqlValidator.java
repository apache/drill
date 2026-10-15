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

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperatorTable;
import org.apache.calcite.sql.validate.SqlValidatorCatalogReader;
import org.apache.calcite.sql.validate.SqlValidatorImpl;
import org.apache.calcite.sql.validate.SqlValidatorScope;

/** Gives bound plan-cache parameters a type during SQL validation. */
final class DrillSqlValidator extends SqlValidatorImpl {
  DrillSqlValidator(SqlOperatorTable operators, SqlValidatorCatalogReader catalog,
      RelDataTypeFactory types, Config config) {
    super(operators, catalog, types, config);
  }

  private RelDataType boundType(SqlNode node) {
    SqlBoundDynamicParam param = (SqlBoundDynamicParam) node;
    return param.getLiteral().createSqlType(typeFactory);
  }

  @Override
  public RelDataType getValidatedNodeTypeIfKnown(SqlNode node) {
    RelDataType type = super.getValidatedNodeTypeIfKnown(node);
    if (type == null && node instanceof SqlBoundDynamicParam) {
      type = boundType(node);
      setValidatedNodeType(node, type);
    }
    return type;
  }

  @Override
  public RelDataType deriveType(SqlValidatorScope scope, SqlNode node) {
    if (node instanceof SqlBoundDynamicParam) {
      return getValidatedNodeType(node);
    }
    return super.deriveType(scope, node);
  }

  @Override
  protected void inferUnknownTypes(RelDataType inferredType, SqlValidatorScope scope, SqlNode node) {
    if (node instanceof SqlBoundDynamicParam) {
      // Unlike an unbound JDBC parameter, this slot already has a concrete
      // non-null SQL literal. Do not reject it when the parent has no type.
      getValidatedNodeType(node);
      return;
    }
    super.inferUnknownTypes(inferredType, scope, node);
  }
}
