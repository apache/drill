package org.apache.drill.exec.store.parquet;

import java.io.IOException;

import org.apache.drill.common.config.DrillConfig;
import org.apache.drill.exec.physical.ReadEntryWithPath;
import org.apache.drill.exec.store.SchemaProvider;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.beust.jcommander.internal.Lists;

public class ParquetSchemaProvider implements SchemaProvider{
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(ParquetSchemaProvider.class);

  public static final String HADOOP_DEFAULT_NAME = "fs.default.name";
  final ParquetStorageEngineConfig configuration;
  final FileSystem fs;
  final Configuration conf;

  public ParquetSchemaProvider(ParquetStorageEngineConfig configuration, DrillConfig config){
    this.configuration = configuration;
    try {
      this.conf = new Configuration();
      this.conf.set(HADOOP_DEFAULT_NAME, "file:///");
      this.fs = FileSystem.get(conf);
    } catch (IOException ie) {
      throw new RuntimeException("Error setting up filesystem");
    }
  }

  @Override
  public Object getSelectionBaseOnName(String tableName) {
    try{
//      if(!fs.exists(new Path(tableName))) return null;
      ReadEntryWithPath re = new ReadEntryWithPath(tableName);
      return Lists.newArrayList(re);
    }catch(Exception e){
      logger.warn(String.format("Failure while checking table name %s.", tableName), e);
      return null;
    }
  }
}
