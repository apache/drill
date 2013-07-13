package org.apache.drill.exec.store.orc;

import org.apache.drill.common.logical.data.Scan;
import org.apache.drill.exec.physical.base.AbstractGroupScan;
import org.apache.drill.exec.store.AbstractStorageEngine;
import org.apache.drill.exec.store.SchemaProvider;

import java.io.IOException;

public class OrcStorageEngine extends AbstractStorageEngine {
  private final OrcSchemaProvider schemaProvider;

  public OrcStorageEngine() {
    schemaProvider = new OrcSchemaProvider();
  }

  @Override
  public SchemaProvider getSchemaProvider() {
    return schemaProvider;
  }

  @Override
  public AbstractGroupScan getPhysicalScan(Scan scan) throws IOException {
    return new OrcGroupScan();
  }
}
