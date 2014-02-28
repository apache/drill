/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.drill.exec.store.parquet;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.regex.Pattern;

import org.apache.drill.common.expression.FieldReference;
import org.apache.drill.common.logical.StoragePluginConfig;
import org.apache.drill.exec.server.DrillbitContext;
import org.apache.drill.exec.store.QueryOptimizerRule;
import org.apache.drill.exec.store.dfs.BasicFormatMatcher;
import org.apache.drill.exec.store.dfs.FileSelection;
import org.apache.drill.exec.store.dfs.FormatMatcher;
import org.apache.drill.exec.store.dfs.FormatPlugin;
import org.apache.drill.exec.store.dfs.FormatSelection;
import org.apache.drill.exec.store.dfs.MagicString;
import org.apache.drill.exec.store.dfs.shim.DrillFileSystem;
import org.apache.drill.exec.store.mock.MockStorageEngine;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;

import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.mapred.Utils;
import parquet.format.converter.ParquetMetadataConverter;
import parquet.hadoop.CodecFactoryExposer;
import parquet.hadoop.ParquetFileWriter;

import com.google.common.collect.Lists;

public class ParquetFormatPlugin implements FormatPlugin{
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(MockStorageEngine.class);

  private final DrillbitContext context;
  static final ParquetMetadataConverter parquetMetadataConverter = new ParquetMetadataConverter();
  private CodecFactoryExposer codecFactoryExposer;
  private final DrillFileSystem fs;
  private final ParquetFormatMatcher formatMatcher;
  private final ParquetFormatConfig config;
  private final StoragePluginConfig storageConfig;
  private final String name;
  
  public ParquetFormatPlugin(String name, DrillbitContext context, DrillFileSystem fs, StoragePluginConfig storageConfig){
    this(name, context, fs, storageConfig, new ParquetFormatConfig());
  }
  
  public ParquetFormatPlugin(String name, DrillbitContext context, DrillFileSystem fs, StoragePluginConfig storageConfig, ParquetFormatConfig formatConfig){
    this.context = context;
    this.codecFactoryExposer = new CodecFactoryExposer(fs.getUnderlying().getConf());
    this.config = formatConfig;
    this.formatMatcher = new ParquetFormatMatcher(this, fs);
    this.storageConfig = storageConfig;
    this.fs = fs;
    this.name = name == null ? "parquet" : name;
  }

  Configuration getHadoopConfig() {
    return fs.getUnderlying().getConf();
  }

  public DrillFileSystem getFileSystem() {
    return fs;
  }

  @Override
  public ParquetFormatConfig getConfig() {
    return config;
  }

  public DrillbitContext getContext() {
    return this.context;
  }

  @Override
  public boolean supportsRead() {
    return true;
  }
  
  @Override
  public List<QueryOptimizerRule> getOptimizerRules() {
    return Collections.emptyList();
  }

  @Override
  public ParquetGroupScan getGroupScan(FieldReference outputRef, FileSelection selection) throws IOException {
    return new ParquetGroupScan( selection.getFileStatusList(fs), this, outputRef);
  }

  @Override
  public StoragePluginConfig getStorageConfig() {
    return storageConfig;
  }

  public CodecFactoryExposer getCodecFactoryExposer() {
    return codecFactoryExposer;
  }

  public String getName(){
    return name;
  }
  
  @Override
  public boolean supportsWrite() {
    return false;
  }



  @Override
  public FormatMatcher getMatcher() {
    return formatMatcher;
  }

  private static class ParquetFormatMatcher extends BasicFormatMatcher{
    
    private final DrillFileSystem fs;
    
    public ParquetFormatMatcher(ParquetFormatPlugin plugin, DrillFileSystem fs) {
      super(plugin, fs, //
          Lists.newArrayList( //
              Pattern.compile(".*\\.parquet$"), //
              Pattern.compile(".*/" + ParquetFileWriter.PARQUET_METADATA_FILE) //
              //
              ),
          Lists.newArrayList(new MagicString(0, ParquetFileWriter.MAGIC))
                    
          );
      this.fs = fs;
      
    }
    
    @Override
    public boolean supportDirectoryReads() {
      return true;
    }

    @Override
    public FormatSelection isReadable(FileSelection selection) throws IOException {
      // TODO: we only check the first file for directory reading.  This is because 
      if(selection.containsDirectories(fs)){
        if(isDirReadable(selection.getFirstPath(fs))){
          return new FormatSelection(plugin.getConfig(), selection);
        }
      }
      return super.isReadable(selection);
    }
    
    boolean isDirReadable(FileStatus dir) {
      Path p = new Path(dir.getPath(), ParquetFileWriter.PARQUET_METADATA_FILE);
      try {
        if (fs.getUnderlying().exists(p)) {
          return true;
        } else {

          PathFilter filter = new Utils.OutputFileUtils.OutputFilesFilter() {
            @Override
            public boolean accept(Path path) {
              if (path.toString().contains("_metadata")) {
                return false;
              }
              return super.accept(path);
            }
          };

          FileStatus[] files = fs.getUnderlying().listStatus(dir.getPath(), filter);
          if (files.length == 0) {
            return false;
          }
          return super.isReadable(files[0]);
        }
      } catch (IOException e) {
        logger.info("Failure while attempting to check for Parquet metadata file.", e);
        return false;
      }
    }
    
    
    
  }
  
}