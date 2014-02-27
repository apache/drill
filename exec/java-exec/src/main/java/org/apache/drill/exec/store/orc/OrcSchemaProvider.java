package org.apache.drill.exec.store.orc;

import org.apache.drill.exec.store.ClassPathFileSystem;
import org.apache.drill.exec.store.SchemaProvider;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;

import java.io.IOException;

public class OrcSchemaProvider implements SchemaProvider {
  private final OrcStorageEngineConfig configuration;
  private final Configuration conf;
  public static final String HADOOP_DEFAULT_NAME = "fs.default.name";
  private final FileSystem fs;

  public OrcSchemaProvider(OrcStorageEngineConfig configuration) {
    this.configuration = configuration;
    try {
      this.conf = new Configuration();
      this.conf.set("fs.classpath.impl", ClassPathFileSystem.class.getName());
      this.conf.set(HADOOP_DEFAULT_NAME, configuration.getDfsName());
      logger.debug("{}: {}",HADOOP_DEFAULT_NAME, configuration.getDfsName());
      this.fs = FileSystem.get(conf);
    } catch (IOException ie) {
      throw new RuntimeException("Error setting up filesystem", ie);
    }
  }

  @Override
  public Object getSelectionBaseOnName(String tableName) {
    return null;
  }

  public FileSystem getFileSystem() {
    return fs;
  }
}
