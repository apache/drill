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
package org.apache.drill.exec.planner.sql.handlers;

import org.apache.calcite.schema.Table;
import org.apache.calcite.tools.Planner;
import org.apache.calcite.tools.RelConversionException;
import org.apache.drill.common.exceptions.DrillException;
import org.apache.drill.exec.planner.common.DrillRelOptUtil;
import org.apache.drill.exec.planner.sql.DirectPlan;
import org.apache.drill.exec.planner.types.DrillFixedRelDataTypeImpl;
import org.apache.drill.exec.store.AbstractSchema;

import org.apache.calcite.tools.ValidationException;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlNode;
import org.apache.drill.exec.store.ischema.Records;

import java.util.List;

public class SqlHandlerUtil {

  /**
   * Resolve final RelNode of the new table (or view) for given table field list and new table definition.
   *
   * @param isNewTableView Is the new table created a view? This doesn't affect the functionality, but it helps format
   *                       better error messages.
   * @param planner Planner instance.
   * @param tableFieldNames List of fields specified in new table/view field list. These are the fields given just after
   *                        new table name.
   *                        Ex. CREATE TABLE newTblName(col1, medianOfCol2, avgOfCol3) AS
   *                        SELECT col1, median(col2), avg(col3) FROM sourcetbl GROUP BY col1;
   * @param newTableQueryDef  Sql tree of definition of the new table or view (query after the AS keyword). This tree is
   *                          modified, so it is responsibility of caller's to make a copy if needed.
   * @throws ValidationException If table's fields list and field list specified in table definition are not valid.
   * @throws RelConversionException If failed to convert the table definition into a RelNode.
   */
  public static RelNode resolveNewTableRel(boolean isNewTableView, Planner planner, List<String> tableFieldNames,
      SqlNode newTableQueryDef) throws ValidationException, RelConversionException {

    SqlNode validatedQuery = planner.validate(newTableQueryDef);
    RelNode validatedQueryRelNode = planner.convert(validatedQuery);

    if (tableFieldNames.size() > 0) {
      final RelDataType queryRowType = validatedQueryRelNode.getRowType();

      // Field count should match.
      if (tableFieldNames.size() != queryRowType.getFieldCount()) {
        final String tblType = isNewTableView ? "view" : "table";
        throw new ValidationException(
            String.format("%s's field list and the %s's query field list have different counts.", tblType, tblType));
      }

      // CTAS's query field list shouldn't have "*" when table's field list is specified.
      for (String field : queryRowType.getFieldNames()) {
        if (field.equals("*")) {
          final String tblType = isNewTableView ? "view" : "table";
          throw new ValidationException(
              String.format("%s's query field list has a '*', which is invalid when %s's field list is specified.",
                  tblType, tblType));
        }
      }

      // CTAS statement has table field list (ex. below), add a project rel to rename the query fields.
      // Ex. CREATE TABLE tblname(col1, medianOfCol2, avgOfCol3) AS
      //        SELECT col1, median(col2), avg(col3) FROM sourcetbl GROUP BY col1 ;
      // Similary for CREATE VIEW.

      return DrillRelOptUtil.createRename(validatedQueryRelNode, tableFieldNames);
    }

    return validatedQueryRelNode;
  }

  public static Table getTableFromSchema(AbstractSchema drillSchema, String tblName) throws DrillException {
    try {
      return drillSchema.getTable(tblName);
    } catch (Exception e) {
      // TODO: Move to better exception types.
      throw new DrillException(
          String.format("Failure while trying to check if a table or view with given name [%s] already exists " +
              "in schema [%s]: %s", tblName, drillSchema.getFullSchemaName(), e.getMessage()), e);
    }
  }
}
