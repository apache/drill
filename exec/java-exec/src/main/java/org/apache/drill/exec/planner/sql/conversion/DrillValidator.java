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

import java.util.List;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperatorTable;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.validate.SqlConformance;
import org.apache.calcite.sql.validate.SqlValidatorCatalogReader;
import org.apache.calcite.sql.validate.SqlValidatorImpl;
import org.apache.calcite.sql.validate.SqlValidatorScope;
import org.apache.calcite.sql.validate.SqlValidatorUtil;
import org.apache.calcite.util.Static;
import org.apache.drill.shaded.guava.com.google.common.collect.Lists;

class DrillValidator extends SqlValidatorImpl {

  DrillValidator(SqlOperatorTable opTab, SqlValidatorCatalogReader catalogReader,
                 RelDataTypeFactory typeFactory, SqlConformance conformance) {
    super(opTab, catalogReader, typeFactory, conformance);
  }

  @Override
  protected void validateFrom(SqlNode node, RelDataType targetRowType, SqlValidatorScope scope) {
    if (node.getKind() == SqlKind.AS) {
      SqlCall sqlCall = (SqlCall) node;
      SqlNode sqlNode = sqlCall.operand(0);
      switch (sqlNode.getKind()) {
        case IDENTIFIER:
          SqlIdentifier tempNode = (SqlIdentifier) sqlNode;
          changeNamesIfTableIsTemporary(tempNode);
          // Check the schema and throw a valid SchemaNotFound exception instead of TableNotFound exception.
          ((DrillCalciteCatalogReader) getCatalogReader()).isValidSchema(tempNode.names);
          break;
        case UNNEST:
          if (sqlCall.operandCount() < 3) {
            throw Static.RESOURCE.validationError("Alias table and column name are required for UNNEST").ex();
          }
      }
    }
    super.validateFrom(node, targetRowType, scope);
  }

  @Override
  public String deriveAlias(SqlNode node, int ordinal) {
    if (node instanceof SqlIdentifier) {
      changeNamesIfTableIsTemporary(((SqlIdentifier) node));
    }
    return SqlValidatorUtil.getAlias(node, ordinal);
  }

  @Override
  protected void inferUnknownTypes(RelDataType inferredType, SqlValidatorScope scope, SqlNode node) {
    // calls validateQuery() for SqlSelect to be sure that temporary table name will be changed
    // for the case when it is used in sub-select
    if (node.getKind() == SqlKind.SELECT) {
      validateQuery(node, scope, inferredType);
    }
    super.inferUnknownTypes(inferredType, scope, node);
  }

  private void changeNamesIfTableIsTemporary(SqlIdentifier tempNode) {
    List<String> temporaryTableNames = ((DrillCalciteCatalogReader) getCatalogReader()).getTemporaryNames(tempNode.names);
    if (temporaryTableNames != null) {
      SqlParserPos pos = tempNode.getComponentParserPosition(0);
      List<SqlParserPos> poses = Lists.newArrayList();
      for (int i = 0; i < temporaryTableNames.size(); i++) {
        poses.add(i, pos);
      }
      tempNode.setNames(temporaryTableNames, poses);
    }
  }
}
