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

import java.io.IOException;

import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Table;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.tools.RelConversionException;
import org.apache.calcite.tools.ValidationException;
import org.apache.drill.common.exceptions.UserException;
import org.apache.drill.exec.physical.PhysicalPlan;
import org.apache.drill.exec.planner.sql.DirectPlan;
import org.apache.drill.exec.planner.sql.SchemaUtilites;
import org.apache.drill.exec.planner.sql.parser.SqlDropTable;
import org.apache.drill.exec.store.AbstractSchema;

// SqlHandler for dropping a table.
public class DropTableHandler extends DefaultSqlHandler {

  private static org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(DropTableHandler.class);

  public DropTableHandler(SqlHandlerConfig config) {
    super(config);
  }

  /**
   * Function resolves the schema and invokes the drop method
   * (while IF EXISTS statement is used function invokes the drop method only if table exists).
   * Raises an exception if the schema is immutable.
   * @param sqlNode - SqlDropTable (SQL parse tree of drop table [if exists] query)
   * @return - Single row indicating drop succeeded or table is not found while IF EXISTS statement is used,
   * raise exception otherwise
   * @throws ValidationException
   * @throws RelConversionException
   * @throws IOException
   */
  @Override
  public PhysicalPlan getPlan(SqlNode sqlNode) throws ValidationException, RelConversionException, IOException {

    SqlDropTable dropTableNode = ((SqlDropTable) sqlNode);
    SqlIdentifier tableIdentifier = dropTableNode.getTableIdentifier();

    SchemaPlus defaultSchema = config.getConverter().getDefaultSchema();
    AbstractSchema drillSchema = null;

    if (tableIdentifier != null) {
      drillSchema = SchemaUtilites.resolveToMutableDrillSchema(defaultSchema, dropTableNode.getSchema());
    }

    String tableName = dropTableNode.getName();
    if (drillSchema == null) {
      throw UserException.validationError()
          .message("Invalid table_name [%s]", tableName)
          .build(logger);
    }

    if (dropTableNode.checkTableExistence()) {
      final Table tableToDrop = SqlHandlerUtil.getTableFromSchema(drillSchema, tableName);
      if (tableToDrop == null || tableToDrop.getJdbcTableType() != Schema.TableType.TABLE) {
        return DirectPlan.createDirectPlan(context, true,
            String.format("Table [%s] not found", tableName));
      }
    }

    drillSchema.dropTable(tableName);

    return DirectPlan.createDirectPlan(context, true,
        String.format("Table [%s] %s", tableName, "dropped"));
  }
}
