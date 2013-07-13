package org.apache.drill.exec.store.orc;

import org.apache.drill.exec.store.SchemaProvider;

public class OrcSchemaProvider implements SchemaProvider {
  @Override
  public Object getSelectionBaseOnName(String tableName) {
    return null;
  }
}
