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
import org.apache.drill.exec.exception.OutOfMemoryException;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.physical.base.AbstractWriter;
import org.apache.drill.exec.physical.base.PhysicalOperator;
import org.apache.drill.exec.physical.impl.WriterRecordBatch;
import org.apache.drill.exec.planner.logical.DrillTable;
import org.apache.drill.exec.planner.logical.DynamicDrillTable;
import org.apache.drill.exec.proto.ExecProtos.FragmentHandle;
import org.apache.drill.exec.record.RecordBatch;
import org.apache.drill.exec.server.DrillbitContext;
import org.apache.drill.exec.store.RecordWriter;
import org.apache.drill.exec.store.StoragePluginOptimizerRule;
import org.apache.drill.exec.store.dfs.BasicFormatMatcher;
import org.apache.drill.exec.store.dfs.DrillFileSystem;
import org.apache.drill.exec.store.dfs.DrillPathFilter;
import org.apache.drill.exec.store.dfs.FileSelection;
import org.apache.drill.exec.store.dfs.FileSystemConfig;
import org.apache.drill.exec.store.dfs.FileSystemPlugin;
import org.apache.drill.exec.store.dfs.FormatMatcher;
import org.apache.drill.exec.store.dfs.FormatPlugin;
import org.apache.drill.exec.store.dfs.FormatSelection;
import org.apache.drill.exec.store.dfs.MagicString;
import org.apache.drill.exec.store.mock.MockStorageEngine;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.parquet.format.converter.ParquetMetadataConverter;
import org.apache.parquet.hadoop.ParquetFileWriter;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

public class ParquetFormatPlugin implements FormatPlugin{
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(MockStorageEngine.class);

  public static final ParquetMetadataConverter parquetMetadataConverter = new ParquetMetadataConverter();

  private static final String DEFAULT_NAME = "parquet";

  private static final List<Pattern> PATTERNS = Lists.newArrayList(
      Pattern.compile(".*\\.parquet$"),
      Pattern.compile(".*/" + ParquetFileWriter.PARQUET_METADATA_FILE));
  private static final List<MagicString> MAGIC_STRINGS = Lists.newArrayList(new MagicString(0, ParquetFileWriter.MAGIC));

  private final DrillbitContext context;
  private final Configuration fsConf;
  private final ParquetFormatMatcher formatMatcher;
  private final ParquetFormatConfig config;
  private final StoragePluginConfig storageConfig;
  private final String name;

  public ParquetFormatPlugin(String name, DrillbitContext context, Configuration fsConf,
      StoragePluginConfig storageConfig){
    this(name, context, fsConf, storageConfig, new ParquetFormatConfig());
  }

  public ParquetFormatPlugin(String name, DrillbitContext context, Configuration fsConf,
      StoragePluginConfig storageConfig, ParquetFormatConfig formatConfig){
    this.context = context;
    this.config = formatConfig;
    this.formatMatcher = new ParquetFormatMatcher(this);
    this.storageConfig = storageConfig;
    this.fsConf = fsConf;
    this.name = name == null ? DEFAULT_NAME : name;
  }

  @Override
  public Configuration getFsConf() {
    return fsConf;
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
  public AbstractWriter getWriter(PhysicalOperator child, String location, List<String> partitionColumns) throws IOException {
    return new ParquetWriter(child, location, partitionColumns, this);
  }

  public RecordWriter getRecordWriter(FragmentContext context, ParquetWriter writer) throws IOException, OutOfMemoryException {
    Map<String, String> options = Maps.newHashMap();

    options.put("location", writer.getLocation());

    FragmentHandle handle = context.getHandle();
    String fragmentId = String.format("%d_%d", handle.getMajorFragmentId(), handle.getMinorFragmentId());
    options.put("prefix", fragmentId);

    options.put(FileSystem.FS_DEFAULT_NAME_KEY, ((FileSystemConfig)writer.getStorageConfig()).connection);

    options.put(ExecConstants.PARQUET_BLOCK_SIZE, context.getOptions().getOption(ExecConstants.PARQUET_BLOCK_SIZE).num_val.toString());
    options.put(ExecConstants.PARQUET_PAGE_SIZE, context.getOptions().getOption(ExecConstants.PARQUET_PAGE_SIZE).num_val.toString());
    options.put(ExecConstants.PARQUET_DICT_PAGE_SIZE, context.getOptions().getOption(ExecConstants.PARQUET_DICT_PAGE_SIZE).num_val.toString());

    options.put(ExecConstants.PARQUET_WRITER_COMPRESSION_TYPE,
        context.getOptions().getOption(ExecConstants.PARQUET_WRITER_COMPRESSION_TYPE).string_val);

    options.put(ExecConstants.PARQUET_WRITER_ENABLE_DICTIONARY_ENCODING,
        context.getOptions().getOption(ExecConstants.PARQUET_WRITER_ENABLE_DICTIONARY_ENCODING).bool_val.toString());

    RecordWriter recordWriter = new ParquetRecordWriter(context, writer);
    recordWriter.init(options);

    return recordWriter;
  }

  public WriterRecordBatch getWriterBatch(FragmentContext context, RecordBatch incoming, ParquetWriter writer)
          throws ExecutionSetupException {
    try {
      return new WriterRecordBatch(writer, incoming, context, getRecordWriter(context, writer));
    } catch(IOException e) {
      throw new ExecutionSetupException(String.format("Failed to create the WriterRecordBatch. %s", e.getMessage()), e);
    }
  }

  @Override
  public ParquetGroupScan getGroupScan(String userName, FileSelection selection, List<SchemaPath> columns)
      throws IOException {
    return new ParquetGroupScan(userName, selection, this, selection.selectionRoot, columns);
  }

  @Override
  public StoragePluginConfig getStorageConfig() {
    return storageConfig;
  }

  public String getName(){
    return name;
  }

  @Override
  public boolean supportsWrite() {
    return false;
  }

  @Override
  public boolean supportsAutoPartitioning() {
    return true;
  }


  @Override
  public FormatMatcher getMatcher() {
    return formatMatcher;
  }

  private static class ParquetFormatMatcher extends BasicFormatMatcher{

    public ParquetFormatMatcher(ParquetFormatPlugin plugin) {
      super(plugin, PATTERNS, MAGIC_STRINGS);
    }

    @Override
    public boolean supportDirectoryReads() {
      return true;
    }

    @Override
    public DrillTable isReadable(DrillFileSystem fs, FileSelection selection,
        FileSystemPlugin fsPlugin, String storageEngineName, String userName)
        throws IOException {
      // TODO: we only check the first file for directory reading.  This is because
      if(selection.containsDirectories(fs)){
        if(isDirReadable(fs, selection.getFirstPath(fs))){
          return new DynamicDrillTable(fsPlugin, storageEngineName, userName,
              new FormatSelection(plugin.getConfig(), expandSelection(fs, selection)));
        }
      }
      return super.isReadable(fs, selection, fsPlugin, storageEngineName, userName);
    }

    private FileSelection expandSelection(DrillFileSystem fs, FileSelection selection) throws IOException {
      if (metaDataFileExists(fs, selection.getFirstPath(fs))) {
        FileStatus metaRootDir = selection.getFirstPath(fs);
        Path metaFilePath = getMetadataPath(metaRootDir);

        // get the metadata for the directory by reading the metadata file
        Metadata.ParquetTableMetadataBase metadata  = Metadata.readBlockMeta(fs, metaFilePath.toString());
        List<String> fileNames = Lists.newArrayList();
        for (Metadata.ParquetFileMetadata file : metadata.getFiles()) {
          fileNames.add(file.getPath());
        }
        // when creating the file selection, set the selection root in the form /a/b instead of
        // file:/a/b.  The reason is that the file names above have been created in the form
        // /a/b/c.parquet and the format of the selection root must match that of the file names
        // otherwise downstream operations such as partition pruning can break.
        final Path metaRootPath = Path.getPathWithoutSchemeAndAuthority(metaRootDir.getPath());
        final FileSelection newSelection = new FileSelection(selection.getStatuses(fs), fileNames, metaRootPath.toString());
        return ParquetFileSelection.create(newSelection, metadata);
      } else {
        // don't expand yet; ParquetGroupScan's metadata gathering operation
        // does that.
        return selection;
      }
    }

    private Path getMetadataPath(FileStatus dir) {
      return new Path(dir.getPath(), Metadata.METADATA_FILENAME);
    }

    private boolean metaDataFileExists(FileSystem fs, FileStatus dir) throws IOException {
      return fs.exists(getMetadataPath(dir));
    }

    boolean isDirReadable(DrillFileSystem fs, FileStatus dir) {
      Path p = new Path(dir.getPath(), ParquetFileWriter.PARQUET_METADATA_FILE);
      try {
        if (fs.exists(p)) {
          return true;
        } else {

          if (metaDataFileExists(fs, dir)) {
            return true;
          }
          PathFilter filter = new DrillPathFilter();

          FileStatus[] files = fs.listStatus(dir.getPath(), filter);
          if (files.length == 0) {
            return false;
          }
          return super.isFileReadable(fs, files[0]);
        }
      } catch (IOException e) {
        logger.info("Failure while attempting to check for Parquet metadata file.", e);
        return false;
      }
    }
  }
}
