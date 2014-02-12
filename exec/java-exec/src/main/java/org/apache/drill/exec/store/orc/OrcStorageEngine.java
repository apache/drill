package org.apache.drill.exec.store.orc;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.drill.common.logical.data.Scan;
import org.apache.drill.exec.physical.ReadEntryWithPath;
import org.apache.drill.exec.server.DrillbitContext;
import org.apache.drill.exec.store.AbstractStorageEngine;
import org.apache.drill.exec.store.SchemaProvider;
import org.apache.hadoop.fs.FileSystem;

import java.io.IOException;
import java.util.ArrayList;

public class OrcStorageEngine extends AbstractStorageEngine {
  private final OrcSchemaProvider schemaProvider;
  private OrcStorageEngineConfig engineConfig;
  private DrillbitContext context;

  public OrcStorageEngine(OrcStorageEngineConfig config, DrillbitContext context) {
    this.engineConfig = config;
    this.context = context;
    schemaProvider = new OrcSchemaProvider(config);
  }

  @Override
  public SchemaProvider getSchemaProvider() {
    return schemaProvider;
  }

  @Override
  public OrcGroupScan getPhysicalScan(Scan scan) throws IOException {
    ArrayList<ReadEntryWithPath> readEntries = scan.getSelection().getListWith(new ObjectMapper(),
        new TypeReference<ArrayList<ReadEntryWithPath>>() {});

    return new OrcGroupScan(readEntries, this, scan.getOutputReference());
  }

  public OrcStorageEngineConfig getEngineConfig() {
    return engineConfig;
  }

  public DrillbitContext getContext() {
    return context;
  }

  public FileSystem getFileSystem() {
    return schemaProvider.getFileSystem();
  }
}
