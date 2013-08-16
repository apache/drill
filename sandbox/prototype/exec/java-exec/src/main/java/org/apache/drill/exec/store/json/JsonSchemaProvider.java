package org.apache.drill.exec.store.json;

import java.io.IOException;

import org.apache.drill.common.config.DrillConfig;
import org.apache.drill.exec.physical.ReadEntryWithPath;
import org.apache.drill.exec.store.SchemaProvider;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;

import com.beust.jcommander.internal.Lists;

public class JsonSchemaProvider implements SchemaProvider{
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(JsonSchemaProvider.class);

  public static final String HADOOP_DEFAULT_NAME = "fs.default.name";
  final JSONStorageEngineConfig configuration;
  final FileSystem fs;
  final Configuration conf;

  public JsonSchemaProvider(JSONStorageEngineConfig configuration, DrillConfig config){
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
