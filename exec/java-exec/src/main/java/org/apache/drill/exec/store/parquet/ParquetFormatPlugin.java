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
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.common.logical.StoragePluginConfig;
import org.apache.drill.exec.ExecConstants;
import org.apache.drill.exec.memory.OutOfMemoryException;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.physical.base.AbstractWriter;
import org.apache.drill.exec.physical.base.PhysicalOperator;
import org.apache.drill.exec.physical.impl.WriterRecordBatch;
import org.apache.drill.exec.proto.ExecProtos.FragmentHandle;
import org.apache.drill.exec.record.RecordBatch;
import org.apache.drill.exec.server.DrillbitContext;
import org.apache.drill.exec.store.RecordWriter;
import org.apache.drill.exec.store.StoragePluginOptimizerRule;
import org.apache.drill.exec.store.dfs.BasicFormatMatcher;
import org.apache.drill.exec.store.dfs.DrillPathFilter;
import org.apache.drill.exec.store.dfs.FileSelection;
import org.apache.drill.exec.store.dfs.FileSystemConfig;
import org.apache.drill.exec.store.dfs.FormatMatcher;
import org.apache.drill.exec.store.dfs.FormatPlugin;
import org.apache.drill.exec.store.dfs.FormatSelection;
import org.apache.drill.exec.store.dfs.MagicString;
import org.apache.drill.exec.store.dfs.DrillFileSystem;
import org.apache.drill.exec.store.mock.MockStorageEngine;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;

import parquet.format.converter.ParquetMetadataConverter;
import parquet.hadoop.CodecFactoryExposer;
import parquet.hadoop.ParquetFileWriter;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

public class ParquetFormatPlugin implements FormatPlugin{
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(MockStorageEngine.class);

  private final DrillbitContext context;
  public static final ParquetMetadataConverter parquetMetadataConverter = new ParquetMetadataConverter();
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
    this.codecFactoryExposer = new CodecFactoryExposer(fs.getConf());
    this.config = formatConfig;
    this.formatMatcher = new ParquetFormatMatcher(this, fs);
    this.storageConfig = storageConfig;
    this.fs = fs;
    this.name = name == null ? "parquet" : name;
  }

  Configuration getHadoopConfig() {
    return fs.getConf();
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
  public Set<StoragePluginOptimizerRule> getOptimizerRules() {
    return ImmutableSet.of();
  }

  @Override
  public AbstractWriter getWriter(PhysicalOperator child, String location) throws IOException {
    return new ParquetWriter(child, location, this);
  }

  public RecordWriter getRecordWriter(FragmentContext context, ParquetWriter writer) throws IOException, OutOfMemoryException {
    Map<String, String> options = Maps.newHashMap();

    options.put("location", writer.getLocation());

    FragmentHandle handle = context.getHandle();
    String fragmentId = String.format("%d_%d", handle.getMajorFragmentId(), handle.getMinorFragmentId());
    options.put("prefix", fragmentId);

    options.put(FileSystem.FS_DEFAULT_NAME_KEY, ((FileSystemConfig)writer.getStorageConfig()).connection);

    options.put(ExecConstants.PARQUET_BLOCK_SIZE, context.getOptions().getOption(ExecConstants.PARQUET_BLOCK_SIZE).num_val.toString());

    options.put(ExecConstants.PARQUET_WRITER_COMPRESSION_TYPE,
        context.getOptions().getOption(ExecConstants.PARQUET_WRITER_COMPRESSION_TYPE).string_val);

    options.put(ExecConstants.PARQUET_WRITER_ENABLE_DICTIONARY_ENCODING,
        context.getOptions().getOption(ExecConstants.PARQUET_WRITER_ENABLE_DICTIONARY_ENCODING).bool_val.toString());

    RecordWriter recordWriter = new ParquetRecordWriter(context, writer);
    recordWriter.init(options);

    return recordWriter;
  }

  public RecordBatch getWriterBatch(FragmentContext context, RecordBatch incoming, ParquetWriter writer)
          throws ExecutionSetupException {
    try {
      return new WriterRecordBatch(writer, incoming, context, getRecordWriter(context, writer));
    } catch(IOException e) {
      throw new ExecutionSetupException(String.format("Failed to create the WriterRecordBatch. %s", e.getMessage()), e);
    }
  }

  @Override
  public ParquetGroupScan getGroupScan(FileSelection selection) throws IOException {
    return new ParquetGroupScan(selection.getFileStatusList(fs), this, selection.selectionRoot, null);
  }

  @Override
  public ParquetGroupScan getGroupScan(FileSelection selection, List<SchemaPath> columns) throws IOException {
    return new ParquetGroupScan(selection.getFileStatusList(fs), this, selection.selectionRoot, columns);
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
        if (fs.exists(p)) {
          return true;
        } else {

          PathFilter filter = new DrillPathFilter();

          FileStatus[] files = fs.listStatus(dir.getPath(), filter);
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