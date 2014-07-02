package org.apache.drill.exec.store.mongo;

import org.apache.drill.exec.planner.logical.DrillTable;
import org.apache.drill.exec.store.StoragePlugin;
import org.eigenbase.reltype.RelDataType;
import org.eigenbase.reltype.RelDataTypeFactory;

public class DrillMongoTable extends DrillTable implements DrillMongoConstants{

  public DrillMongoTable(String storageEngineName, StoragePlugin plugin,
      Object selection) {
    super(storageEngineName, plugin, selection);
  }

  @Override
  public RelDataType getRowType(RelDataTypeFactory arg0) {
    return null;
  }

}
