package org.apache.drill.exec.store.jdbc.clickhouse;

import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.adapter.jdbc.JdbcImplementor;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.SqlDialect;
import org.apache.drill.exec.store.SchemaConfig;
import org.apache.drill.exec.store.SubsetRemover;
import org.apache.drill.exec.store.jdbc.JdbcDialect;
import org.apache.drill.exec.store.jdbc.JdbcStoragePlugin;

public class ClickhouseJdbcDialect extends JdbcDialect {

  public ClickhouseJdbcDialect(JdbcStoragePlugin plugin) {
    super(plugin);
  }

  @Override
  public void registerSchemas(SchemaConfig config, SchemaPlus parent) {
    ClickhouseCatalogSchema schema = new ClickhouseCatalogSchema(plugin.getName(),
      plugin.getDataSource(), plugin.getDialect(), plugin.getConvention());
    SchemaPlus holder = parent.add(plugin.getName(), schema);
    schema.setHolder(holder);
  }

  @Override
  public String generateSql(RelOptCluster cluster, RelNode input) {
    final SqlDialect dialect = plugin.getDialect();
    final JdbcImplementor jdbcImplementor = new ClickhouseJdbcImplementor(dialect,
      (JavaTypeFactory) cluster.getTypeFactory());
    final JdbcImplementor.Result result = jdbcImplementor.visitChild(0,
      input.accept(SubsetRemover.INSTANCE));
    return result.asStatement().toSqlString(dialect).getSql();
  }
}
