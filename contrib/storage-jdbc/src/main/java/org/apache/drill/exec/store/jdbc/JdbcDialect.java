package org.apache.drill.exec.store.jdbc;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.drill.exec.store.SchemaConfig;

/**
 * Interface for different implementations of databases connected using the
 * JdbcStoragePlugin.
 */
public interface JdbcDialect {

  /**
   * Register the schemas provided by this JdbcDialect implementation under the
   * given parent schema.
   *
   * @param config Configuration for schema objects.
   * @param parent Reference to parent schema.
   */
  void registerSchemas(SchemaConfig config, SchemaPlus parent);

  /**
   * Generate sql from relational expressions.
   *
   * @param cluster An environment for related relational expressions.
   * @param input Relational expressions.
   */
  String generateSql(RelOptCluster cluster, RelNode input);
}
