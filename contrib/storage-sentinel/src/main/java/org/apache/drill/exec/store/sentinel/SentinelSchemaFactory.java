package org.apache.drill.exec.store.sentinel;

import org.apache.calcite.schema.SchemaPlus;
import org.apache.drill.exec.store.AbstractSchemaFactory;
import org.apache.drill.exec.store.SchemaConfig;

public class SentinelSchemaFactory extends AbstractSchemaFactory {
  private final SentinelStoragePlugin plugin;

  public SentinelSchemaFactory(SentinelStoragePlugin plugin) {
    super(plugin.getName());
    this.plugin = plugin;
  }

  @Override
  public void registerSchemas(SchemaConfig schemaConfig, SchemaPlus parent) {
    SentinelSchema schema = new SentinelSchema(plugin, getName());
    parent.add(getName(), schema);
  }
}
