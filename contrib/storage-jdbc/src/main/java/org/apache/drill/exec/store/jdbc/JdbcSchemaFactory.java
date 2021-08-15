package org.apache.drill.exec.store.jdbc;

import org.apache.calcite.schema.SchemaPlus;
import org.apache.drill.exec.store.SchemaConfig;
import org.apache.drill.exec.store.jdbc.clickhouse.ClickhouseCatalogSchema;

import static ru.yandex.clickhouse.ClickhouseJdbcUrlParser.JDBC_CLICKHOUSE_PREFIX;

public class JdbcSchemaFactory {
  private final JdbcStoragePlugin plugin;

  public JdbcSchemaFactory (JdbcStoragePlugin plugin) {
    this.plugin = plugin;
  }

  public void registerSchemas(SchemaConfig config, SchemaPlus parent) {
    if (plugin.getConfig().getUrl().startsWith(JDBC_CLICKHOUSE_PREFIX)) {
      ClickhouseCatalogSchema schema = new ClickhouseCatalogSchema(plugin.getName(),
        plugin.getDataSource(), plugin.getDialect(), plugin.getConvention());
      SchemaPlus holder = parent.add(plugin.getName(), schema);
      schema.setHolder(holder);
    } else {
      JdbcCatalogSchema schema = new JdbcCatalogSchema(plugin.getName(),
        plugin.getDataSource(), plugin.getDialect(), plugin.getConvention(),
        !plugin.getConfig().areTableNamesCaseInsensitive());
      SchemaPlus holder = parent.add(plugin.getName(), schema);
      schema.setHolder(holder);
    }
  }
}
