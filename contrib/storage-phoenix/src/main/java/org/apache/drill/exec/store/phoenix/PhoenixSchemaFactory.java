package org.apache.drill.exec.store.phoenix;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import java.util.Set;

import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Table;
import org.apache.drill.common.map.CaseInsensitiveMap;
import org.apache.drill.exec.planner.logical.DynamicDrillTable;
import org.apache.drill.exec.store.AbstractSchema;
import org.apache.drill.exec.store.AbstractSchemaFactory;
import org.apache.drill.exec.store.SchemaConfig;
import org.apache.drill.shaded.guava.com.google.common.collect.Sets;

public class PhoenixSchemaFactory extends AbstractSchemaFactory {

  public static final String MY_TABLE = "myTable";

  private final PhoenixStoragePlugin plugin;

  public PhoenixSchemaFactory(PhoenixStoragePlugin plugin) {
    super(plugin.getName());
    this.plugin = plugin;
  }

  @Override
  public void registerSchemas(SchemaConfig schemaConfig, SchemaPlus parent) throws IOException {
    PhoenixSchema schema = new PhoenixSchema(plugin);
    parent.add(getName(), schema);
  }

  protected static class PhoenixSchema extends AbstractSchema {

    private final Map<String, DynamicDrillTable> activeTables = CaseInsensitiveMap.newHashMap();
    private final PhoenixStoragePlugin plugin;

    public PhoenixSchema(PhoenixStoragePlugin plugin) {
      super(Collections.emptyList(), plugin.getName());
      this.plugin = plugin;
    }

    @Override
    public Table getTable(String name) {
      DynamicDrillTable table = activeTables.get(name);
      if (table != null) {
        return table;
      }
      if (MY_TABLE.contentEquals(name)) {
        return registerTable(name,
            new DynamicDrillTable(plugin, plugin.getName(),
                new PhoenixScanSpec(name)));
      }
      return null;
    }

    private DynamicDrillTable registerTable(String name, DynamicDrillTable table) {
      activeTables.put(name, table);
      return table;
    }

    @Override
    public Set<String> getTableNames() {
      return Sets.newHashSet(MY_TABLE);
    }

    @Override
    public String getTypeName() {
      return PhoenixStoragePluginConfig.NAME;
    }
  }
}
