package org.apache.drill.exec.store.phoenix;

import java.io.IOException;

import org.apache.calcite.schema.SchemaPlus;
import org.apache.drill.common.JSONOptions;
import org.apache.drill.common.logical.StoragePluginConfig;
import org.apache.drill.exec.physical.base.AbstractGroupScan;
import org.apache.drill.exec.server.DrillbitContext;
import org.apache.drill.exec.store.AbstractStoragePlugin;
import org.apache.drill.exec.store.SchemaConfig;

import com.fasterxml.jackson.core.type.TypeReference;

public class PhoenixStoragePlugin extends AbstractStoragePlugin {

  private final PhoenixStoragePluginConfig config;
  private final PhoenixSchemaFactory schemaFactory;

  public PhoenixStoragePlugin(PhoenixStoragePluginConfig config, DrillbitContext context, String name) {
    super(context, name);
    this.config = config;
    this.schemaFactory = new PhoenixSchemaFactory(this);
  }

  @Override
  public StoragePluginConfig getConfig() {
    return config;
  }

  @Override
  public boolean supportsRead() {
    return true;
  }

  @Override
  public void registerSchemas(SchemaConfig schemaConfig, SchemaPlus parent) throws IOException {
    schemaFactory.registerSchemas(schemaConfig, parent);
  }

  @Override
  public AbstractGroupScan getPhysicalScan(String userName, JSONOptions selection) throws IOException {
      PhoenixScanSpec scanSpec = selection.getListWith(context.getLpPersistence().getMapper(), new TypeReference<PhoenixScanSpec>() {});
      return new PhoenixGroupScan(scanSpec);
  }
}
