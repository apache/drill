package org.apache.drill.exec.store.ischema;

import org.apache.drill.common.logical.StoragePluginConfig;
import org.apache.drill.exec.planner.logical.DrillTable;
import org.eigenbase.reltype.RelDataType;
import org.eigenbase.reltype.RelDataTypeFactory;

public class InfoSchemaDrillTable extends DrillTable{
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(InfoSchemaDrillTable.class);

  private final SelectedTable table;
  
  public InfoSchemaDrillTable(String storageEngineName, SelectedTable selection, StoragePluginConfig storageEngineConfig) {
    super(storageEngineName, selection, storageEngineConfig);
    this.table = selection;
  }

  @Override
  public RelDataType getRowType(RelDataTypeFactory typeFactory) {
    return table.getRowType(typeFactory);
  }

}
