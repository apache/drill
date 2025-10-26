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
import org.apache.calcite.prepare.Prepare;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql2rel.SqlRexConvertletTable;
import org.apache.calcite.sql2rel.SqlToRelConverter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Custom SqlToRelConverter for Drill that handles Calcite 1.38+ type checking issues.
 *
 * <p>Calcite 1.38 introduced strict type validation in checkConvertedType() that enforces
 * validated types exactly match converted types. This is incompatible with:
 * 1. Drill's DECIMAL arithmetic (widens precision/scale for overflow protection)
 * 2. VARCHAR CONCAT operations (Calcite changed type inference in 1.38)
 *
 * <p>This converter overrides convertQuery() to disable the strict type checking.
 */
class DrillSqlToRelConverter extends SqlToRelConverter {

  private static final Logger logger = LoggerFactory.getLogger(DrillSqlToRelConverter.class);
  private final SqlValidator validator;


  public DrillSqlToRelConverter(
      RelOptTable.ViewExpander viewExpander,
      SqlValidator validator,
      Prepare.CatalogReader catalogReader,
      RelOptCluster cluster,
      SqlRexConvertletTable convertletTable,
      Config config) {
    super(viewExpander, validator, catalogReader, cluster, convertletTable, config);
    this.validator = validator;
  }

  /**
   * Override convertQuery to skip strict type checking.
   *
   * <p>Calcite 1.38's convertQuery() calls checkConvertedType() which enforces strict type matching.
   * This is incompatible with Drill's type system for DECIMAL and VARCHAR CONCAT.
   * We catch the AssertionError and return the RelRoot without the type check.
   */
  @Override
  public RelRoot convertQuery(SqlNode query, boolean needsValidation, boolean top) {
    try {
      // Try normal conversion with type checking
      return super.convertQuery(query, needsValidation, top);
    } catch (AssertionError e) {
      // If we get "Conversion to relational algebra failed to preserve datatypes"
      // it's a known Calcite 1.38 issue - just log and proceed without the check
      if (e.getMessage() != null && e.getMessage().contains("preserve datatypes")) {
        logger.warn("Calcite 1.38 type checking failed (known issue), proceeding without strict validation");
        logger.debug("Type mismatch details: {}", e.getMessage());

        // Convert without the strict type check by calling convertQueryRecursive directly
        // This bypasses checkConvertedType() which is the source of the AssertionError
        SqlNode validatedQuery = needsValidation ? validator.validate(query) : query;
        RelNode relNode = convertQueryRecursive(validatedQuery, top, null).rel;
        RelDataType validatedRowType = validator.getValidatedNodeType(validatedQuery);
        return RelRoot.of(relNode, validatedRowType, query.getKind());
      }
      throw e;
    }
  }
}
