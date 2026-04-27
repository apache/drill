package org.apache.drill.exec.store.sentinel;

import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Table;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.exec.planner.logical.DynamicDrillTable;
import org.apache.drill.exec.store.AbstractSchema;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class SentinelSchema extends AbstractSchema {
  private final SentinelStoragePlugin plugin;
  private final String schemaName;
  private final Map<String, Table> tableCache;

  public SentinelSchema(SentinelStoragePlugin plugin, String schemaName) {
    super(Collections.emptyList(), schemaName);
    this.plugin = plugin;
    this.schemaName = schemaName;
    this.tableCache = new HashMap<>();
  }

  @Override
  public Table getTable(String name) {
    if (tableCache.containsKey(name)) {
      return tableCache.get(name);
    }

    SentinelScanSpec scanSpec = new SentinelScanSpec(
        plugin.getName(),
        name,
        name);

    SentinelGroupScan groupScan = new SentinelGroupScan(scanSpec, null);

    DynamicDrillTable table = new DynamicDrillTable(
        plugin,
        plugin.getName(),
        scanSpec);

    tableCache.put(name, table);
    return table;
  }

  @Override
  public Set<String> getTableNames() {
    return Set.copyOf(plugin.getTableNames());
  }

  @Override
  public String getTypeName() {
    return "sentinel";
  }
}
