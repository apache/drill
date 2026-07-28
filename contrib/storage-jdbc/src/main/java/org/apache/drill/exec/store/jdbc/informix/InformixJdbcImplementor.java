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
package org.apache.drill.exec.store.jdbc.informix;

import java.util.ArrayList;
import java.util.List;

import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.adapter.jdbc.JdbcImplementor;
import org.apache.calcite.adapter.jdbc.JdbcTable;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.parser.SqlParserPos;

import com.google.common.collect.ImmutableList;

import static java.util.Objects.requireNonNull;

public class InformixJdbcImplementor extends JdbcImplementor {
  public InformixJdbcImplementor(SqlDialect dialect, JavaTypeFactory typeFactory) {
    super(dialect, typeFactory);
  }

  @Override
  public Result visit(TableScan scan) {
    SqlIdentifier sqlIdentifier = getSqlTargetTable(scan);
    return result(sqlIdentifier, ImmutableList.of(Clause.FROM), scan, null);
  }

  static SqlIdentifier getSqlTargetTable(RelNode e) {
    RelOptTable table = requireNonNull(e.getTable());
    return table.maybeUnwrap(JdbcTable.class)
      .map(jdbcTable -> {
        List<String> names = new ArrayList<>(2);
        // Informix JDBC reports the connected database as a catalog, but
        // Informix SQL does not accept it as a dot-qualified table prefix.
        if (jdbcTable.jdbcSchemaName != null) {
          names.add(jdbcTable.jdbcSchemaName);
        }
        names.add(jdbcTable.jdbcTableName);
        return new SqlIdentifier(names, SqlParserPos.ZERO);
      })
      .orElseGet(() -> new SqlIdentifier(table.getQualifiedName(), SqlParserPos.ZERO));
  }
}
