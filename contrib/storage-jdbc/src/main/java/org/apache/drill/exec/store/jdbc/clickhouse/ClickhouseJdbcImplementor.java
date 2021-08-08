package org.apache.drill.exec.store.jdbc.clickhouse;

import org.apache.drill.shaded.guava.com.google.common.base.Preconditions;
import org.apache.drill.shaded.guava.com.google.common.collect.ImmutableList;
import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.adapter.jdbc.JdbcImplementor;
import org.apache.calcite.adapter.jdbc.JdbcTableScan;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.SqlIdentifier;

import java.util.Iterator;

/**
 * @author feiteng.wtf
 * @date 2021-07-26
 */
public class ClickhouseJdbcImplementor extends JdbcImplementor {
  public ClickhouseJdbcImplementor(SqlDialect dialect,
                                   JavaTypeFactory typeFactory) {
    super(dialect, typeFactory);
  }

  @Override
  public Result visit(JdbcTableScan scan) {
    SqlIdentifier sqlIdentifier = scan.jdbcTable.tableName();
    Iterator<String> iter = sqlIdentifier.names.iterator();
    Preconditions.checkArgument(sqlIdentifier.names.size() == 3,
      "size of clickhouse table names:[%s] is not 3", sqlIdentifier.toString());
    iter.next();
    sqlIdentifier.setNames(ImmutableList.copyOf(iter), null);
    return result(sqlIdentifier, ImmutableList.of(Clause.FROM), scan, null);
  }
}
