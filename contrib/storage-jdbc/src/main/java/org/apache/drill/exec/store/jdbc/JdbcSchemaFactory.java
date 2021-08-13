package org.apache.drill.exec.store.jdbc;


import org.apache.calcite.schema.SchemaPlus;
import org.apache.drill.exec.store.SchemaConfig;
import org.apache.drill.exec.store.jdbc.clickhouse.ClickhouseCatalogSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.SQLException;

import static org.apache.drill.exec.store.jdbc.clickhouse.ClickhouseConstant.PRODUCT_NAME;

public class JdbcSchemaFactory {
  private static final Logger logger = LoggerFactory.getLogger(JdbcSchemaFactory.class);

  private final JdbcStoragePlugin plugin;

  public JdbcSchemaFactory (JdbcStoragePlugin plugin) {
    this.plugin = plugin;
  }

  public void registerSchemas(SchemaConfig config, SchemaPlus parent) {
    try (Connection con = plugin.getDataSource().getConnection()) {
      if (PRODUCT_NAME.equals(con.getMetaData().getDatabaseProductName())) {
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
    } catch (SQLException e) {
      logger.warn("Failure while attempting to register JDBC schema.", e);
    }
  }
}
