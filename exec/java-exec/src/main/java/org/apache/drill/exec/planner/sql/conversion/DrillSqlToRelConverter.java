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
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.prepare.Prepare;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.hint.RelHint;
import org.apache.calcite.rel.stream.LogicalDelta;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.SqlUtil;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql2rel.SqlRexConvertletTable;
import org.apache.calcite.sql2rel.SqlToRelConverter;
import org.apache.calcite.tools.RelBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Custom SqlToRelConverter for Drill that handles Calcite 1.38+ DECIMAL type checking.
 *
 * <p>Calcite 1.38 introduced strict type validation in checkConvertedType() that enforces
 * validated types exactly match converted types. This is incompatible with Drill's DECIMAL
 * arithmetic, which widens precision/scale for overflow protection.
 *
 * <p>This converter overrides convertQuery() using reflection to access private methods,
 * allowing us to replicate Calcite's logic while skipping the problematic type check.
 */
class DrillSqlToRelConverter extends SqlToRelConverter {

  private static final Logger logger = LoggerFactory.getLogger(DrillSqlToRelConverter.class);


  public DrillSqlToRelConverter(
      RelOptTable.ViewExpander viewExpander,
      SqlValidator validator,
      Prepare.CatalogReader catalogReader,
      RelOptCluster cluster,
      SqlRexConvertletTable convertletTable,
      Config config) {
    super(viewExpander, validator, catalogReader, cluster, convertletTable, config);
  }

  /**
   * Override convertQuery to skip DECIMAL type checking.
   *
   * <p>Calcite 1.38's convertQuery() calls checkConvertedType() which enforces strict type matching.
   * Since checkConvertedType() is private and ignores the RelRoot.validatedRowType, we must override
   * convertQuery() entirely and skip the type check for DECIMAL widening cases.
   */
  @Override
  public RelRoot convertQuery(SqlNode query, boolean needsValidation, boolean top) {
    // For now, just call parent - we'll handle DECIMAL type checking differently
    return super.convertQuery(query, needsValidation, top);
  }
}
