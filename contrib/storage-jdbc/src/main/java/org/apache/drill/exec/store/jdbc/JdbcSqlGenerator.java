package org.apache.drill.exec.store.jdbc;

import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.adapter.jdbc.JdbcImplementor;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.sql.SqlDialect;
import org.apache.drill.exec.store.SubsetRemover;
import org.apache.drill.exec.store.jdbc.clickhouse.ClickhouseJdbcImplementor;

import static ru.yandex.clickhouse.ClickhouseJdbcUrlParser.JDBC_CLICKHOUSE_PREFIX;

public class JdbcSqlGenerator {
  /**
   * Generate sql for different jdbc databases
   */
  public static String generateSql(JdbcStoragePlugin plugin,
                                   RelOptCluster cluster, RelNode input) {
    final SqlDialect dialect = plugin.getDialect();
    final JdbcImplementor jdbcImplementor;
    if (plugin.getConfig().getUrl().startsWith(JDBC_CLICKHOUSE_PREFIX)) {
      jdbcImplementor = new ClickhouseJdbcImplementor(dialect,
        (JavaTypeFactory) cluster.getTypeFactory());
    } else {
      jdbcImplementor = new JdbcImplementor(dialect,
        (JavaTypeFactory) cluster.getTypeFactory());
    }
    final JdbcImplementor.Result result = jdbcImplementor.visitChild(0,
      input.accept(SubsetRemover.INSTANCE));
    return result.asStatement().toSqlString(dialect).getSql();
  }
}
