/*
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
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.common.expression.LogicalExpression;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.common.expression.ValueExpressions;
import org.apache.drill.common.logical.FormatPluginConfig;
import org.apache.drill.common.logical.StoragePluginConfig;
import org.apache.drill.exec.physical.base.GroupScan;
import org.apache.drill.exec.physical.base.PhysicalOperator;
import org.apache.drill.exec.proto.CoordinationProtos.DrillbitEndpoint;
import org.apache.drill.exec.store.ColumnExplorer;
import org.apache.drill.exec.store.StoragePluginRegistry;
import org.apache.drill.exec.store.dfs.DrillFileSystem;
import org.apache.drill.exec.store.dfs.FileSelection;
import org.apache.drill.exec.store.parquet.metadata.Metadata;
import org.apache.drill.exec.util.DrillFileSystemUtil;
import org.apache.drill.exec.store.dfs.MetadataContext;
import org.apache.drill.exec.store.dfs.MetadataContext.PruneStatus;
import org.apache.drill.exec.store.dfs.ReadEntryWithPath;
import org.apache.drill.exec.store.parquet.metadata.MetadataBase.ParquetFileMetadata;
import org.apache.drill.exec.store.parquet.metadata.MetadataBase.ParquetTableMetadataBase;
import org.apache.drill.exec.util.ImpersonationUtil;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import org.apache.drill.shaded.guava.com.google.common.base.Preconditions;
import org.apache.drill.shaded.guava.com.google.common.collect.Sets;

@JsonTypeName("parquet-scan")
public class ParquetGroupScan extends AbstractParquetGroupScan {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(ParquetGroupScan.class);

  private final ParquetFormatPlugin formatPlugin;
  private final ParquetFormatConfig formatConfig;
  private final DrillFileSystem fs;
  private final MetadataContext metaContext;
  private boolean usedMetadataCache; // false by default
  // may change when filter push down / partition pruning is applied
  private String selectionRoot;
  private String cacheFileRoot;

  @JsonCreator
  public ParquetGroupScan(@JacksonInject StoragePluginRegistry engineRegistry,
                          @JsonProperty("userName") String userName,
                          @JsonProperty("entries") List<ReadEntryWithPath> entries,
                          @JsonProperty("storage") StoragePluginConfig storageConfig,
                          @JsonProperty("format") FormatPluginConfig formatConfig,
                          @JsonProperty("columns") List<SchemaPath> columns,
                          @JsonProperty("selectionRoot") String selectionRoot,
                          @JsonProperty("cacheFileRoot") String cacheFileRoot,
                          @JsonProperty("readerConfig") ParquetReaderConfig readerConfig,
                          @JsonProperty("filter") LogicalExpression filter) throws IOException, ExecutionSetupException {
    super(ImpersonationUtil.resolveUserName(userName), columns, entries, readerConfig, filter);
    Preconditions.checkNotNull(storageConfig);
    Preconditions.checkNotNull(formatConfig);
    this.formatPlugin = (ParquetFormatPlugin) engineRegistry.getFormatPlugin(storageConfig, formatConfig);
    Preconditions.checkNotNull(formatPlugin);
    this.fs = ImpersonationUtil.createFileSystem(getUserName(), formatPlugin.getFsConf());
    this.formatConfig = formatPlugin.getConfig();
    this.selectionRoot = selectionRoot;
    this.cacheFileRoot = cacheFileRoot;
    this.metaContext = new MetadataContext();

    init();
  }

  public ParquetGroupScan(String userName,
                          FileSelection selection,
                          ParquetFormatPlugin formatPlugin,
                          List<SchemaPath> columns,
                          ParquetReaderConfig readerConfig) throws IOException {
    this(userName, selection, formatPlugin, columns, readerConfig, ValueExpressions.BooleanExpression.TRUE);
  }

  public ParquetGroupScan(String userName,
                          FileSelection selection,
                          ParquetFormatPlugin formatPlugin,
                          List<SchemaPath> columns,
                          ParquetReaderConfig readerConfig,
                          LogicalExpression filter) throws IOException {
    super(userName, columns, new ArrayList<>(), readerConfig, filter);

    this.formatPlugin = formatPlugin;
    this.formatConfig = formatPlugin.getConfig();
    this.fs = ImpersonationUtil.createFileSystem(userName, formatPlugin.getFsConf());
    this.selectionRoot = selection.getSelectionRoot();
    this.cacheFileRoot = selection.getCacheFileRoot();

    MetadataContext metadataContext = selection.getMetaContext();
    this.metaContext = metadataContext != null ? metadataContext : new MetadataContext();

    FileSelection fileSelection = expandIfNecessary(selection);
    if (fileSelection != null) {
      if (checkForInitializingEntriesWithSelectionRoot()) {
        // The fully expanded list is already stored as part of the fileSet
        entries.add(new ReadEntryWithPath(fileSelection.getSelectionRoot()));
      } else {
        for (String fileName : fileSelection.getFiles()) {
          entries.add(new ReadEntryWithPath(fileName));
        }
      }

      init();
    }
  }

  private ParquetGroupScan(ParquetGroupScan that) {
    this(that, null);
  }

  private ParquetGroupScan(ParquetGroupScan that, FileSelection selection) {
    super(that);
    this.formatConfig = that.formatConfig;
    this.formatPlugin = that.formatPlugin;
    this.fs = that.fs;
    this.selectionRoot = that.selectionRoot;
    if (selection != null) {
      this.cacheFileRoot = selection.getCacheFileRoot();
      MetadataContext metaContext = selection.getMetaContext();
      this.metaContext = metaContext != null ? metaContext : that.metaContext;
    } else {
      this.cacheFileRoot = that.cacheFileRoot;
      this.metaContext = that.metaContext;
    }
    this.usedMetadataCache = that.usedMetadataCache;
  }

  // getters for serialization / deserialization start
  @JsonProperty("format")
  public ParquetFormatConfig getFormatConfig() {
    return formatConfig;
  }

  @JsonProperty("storage")
  public StoragePluginConfig getEngineConfig() {
    return formatPlugin.getStorageConfig();
  }

  @JsonProperty
  public String getSelectionRoot() {
    return selectionRoot;
  }

  @JsonProperty
  public String getCacheFileRoot() {
    return cacheFileRoot;
  }
  // getters for serialization / deserialization end

  @Override
  public ParquetRowGroupScan getSpecificScan(int minorFragmentId) {
    return new ParquetRowGroupScan(getUserName(), formatPlugin, getReadEntries(minorFragmentId), columns, readerConfig, selectionRoot, filter);
  }

  @Override
  public PhysicalOperator getNewWithChildren(List<PhysicalOperator> children) {
    Preconditions.checkArgument(children.isEmpty());
    return new ParquetGroupScan(this);
  }

  @Override
  public GroupScan clone(List<SchemaPath> columns) {
    ParquetGroupScan newScan = new ParquetGroupScan(this);
    newScan.columns = columns;
    return newScan;
  }

  @Override
  public ParquetGroupScan clone(FileSelection selection) throws IOException {
    ParquetGroupScan newScan = new ParquetGroupScan(this, selection);
    newScan.modifyFileSelection(selection);
    newScan.init();
    return newScan;
  }

  @Override
  public String toString() {
    StringBuilder builder = new StringBuilder();
    builder.append("ParquetGroupScan [");
    builder.append("entries=").append(entries);
    builder.append(", selectionRoot=").append(selectionRoot);
    builder.append(", numFiles=").append(getEntries().size());
    builder.append(", numRowGroups=").append(rowGroupInfos.size());
    builder.append(", usedMetadataFile=").append(usedMetadataCache);

    String filterString = getFilterString();
    if (!filterString.isEmpty()) {
      builder.append(", filter=").append(filterString);
    }

    if (usedMetadataCache) {
      // For EXPLAIN, remove the URI prefix from cacheFileRoot.  If cacheFileRoot is null, we
      // would have read the cache file from selectionRoot
      String cacheFileRootString = (cacheFileRoot == null) ?
          Path.getPathWithoutSchemeAndAuthority(new Path(selectionRoot)).toString() :
          Path.getPathWithoutSchemeAndAuthority(new Path(cacheFileRoot)).toString();
      builder.append(", cacheFileRoot=").append(cacheFileRootString);
    }

    builder.append(", columns=").append(columns);
    builder.append("]");

    return builder.toString();
  }

  // overridden protected methods block start
  @Override
  protected void initInternal() throws IOException {
    FileSystem processUserFileSystem = ImpersonationUtil.createFileSystem(ImpersonationUtil.getProcessUserName(), fs.getConf());
    Path metaPath = null;
    if (entries.size() == 1 && parquetTableMetadata == null) {
      Path p = Path.getPathWithoutSchemeAndAuthority(new Path(entries.get(0).getPath()));
      if (fs.isDirectory(p)) {
        // Using the metadata file makes sense when querying a directory; otherwise
        // if querying a single file we can look up the metadata directly from the file
        metaPath = new Path(p, Metadata.METADATA_FILENAME);
      }
      if (!metaContext.isMetadataCacheCorrupted() && metaPath != null && fs.exists(metaPath)) {
        parquetTableMetadata = Metadata.readBlockMeta(processUserFileSystem, metaPath, metaContext, readerConfig);
        if (parquetTableMetadata != null) {
          usedMetadataCache = true;
        }
      }
      if (!usedMetadataCache) {
        parquetTableMetadata = Metadata.getParquetTableMetadata(processUserFileSystem, p.toString(), readerConfig);
      }
    } else {
      Path p = Path.getPathWithoutSchemeAndAuthority(new Path(selectionRoot));
      metaPath = new Path(p, Metadata.METADATA_FILENAME);
      if (!metaContext.isMetadataCacheCorrupted() && fs.isDirectory(new Path(selectionRoot))
          && fs.exists(metaPath)) {
        if (parquetTableMetadata == null) {
          parquetTableMetadata = Metadata.readBlockMeta(processUserFileSystem, metaPath, metaContext, readerConfig);
        }
        if (parquetTableMetadata != null) {
          usedMetadataCache = true;
          if (fileSet != null) {
            parquetTableMetadata = removeUnneededRowGroups(parquetTableMetadata);
          }
        }
      }
      if (!usedMetadataCache) {
        final List<FileStatus> fileStatuses = new ArrayList<>();
        for (ReadEntryWithPath entry : entries) {
          fileStatuses.addAll(
              DrillFileSystemUtil.listFiles(fs, Path.getPathWithoutSchemeAndAuthority(new Path(entry.getPath())), true));
        }

        Map<FileStatus, FileSystem> statusMap = fileStatuses.stream()
            .collect(
                Collectors.toMap(
                    Function.identity(),
                    s -> processUserFileSystem,
                    (oldFs, newFs) -> newFs,
                    LinkedHashMap::new));

        parquetTableMetadata = Metadata.getParquetTableMetadata(statusMap, readerConfig);
      }
    }
  }

  @Override
  protected AbstractParquetGroupScan cloneWithFileSelection(Collection<String> filePaths) throws IOException {
    FileSelection newSelection = new FileSelection(null, new ArrayList<>(filePaths), getSelectionRoot(), cacheFileRoot, false);
    return clone(newSelection);
  }

  @Override
  protected Collection<DrillbitEndpoint> getDrillbits() {
    return formatPlugin.getContext().getBits();
  }

  @Override
  protected boolean supportsFileImplicitColumns() {
    return selectionRoot != null;
  }

  @Override
  protected List<String> getPartitionValues(RowGroupInfo rowGroupInfo) {
    return ColumnExplorer.listPartitionValues(rowGroupInfo.getPath(), selectionRoot);
  }
  // overridden protected methods block end

  // private methods block start
  /**
   * Expands the selection's folders if metadata cache is found for the selection root.<br>
   * If the selection has already been expanded or no metadata cache was found, does nothing
   *
   * @param selection actual selection before expansion
   * @return new selection after expansion, if no expansion was done returns the input selection
   */
  private FileSelection expandIfNecessary(FileSelection selection) throws IOException {
    if (selection.isExpandedFully()) {
      return selection;
    }

    // use the cacheFileRoot if provided (e.g after partition pruning)
    Path metaFilePath = new Path(cacheFileRoot != null ? cacheFileRoot : selectionRoot, Metadata.METADATA_FILENAME);
    if (!fs.exists(metaFilePath)) { // no metadata cache
      if (selection.isExpandedPartial()) {
        logger.error("'{}' metadata file does not exist, but metadata directories cache file is present", metaFilePath);
        metaContext.setMetadataCacheCorrupted(true);
      }

      return selection;
    }

    return expandSelectionFromMetadataCache(selection, metaFilePath);
  }

  /**
   * For two cases the entries should be initialized with just the selection root instead of the fully expanded list:
   * <ul>
   *   <li> When metadata caching is corrupted (to use correct file selection)
   *   <li> Metadata caching is correct and used, but pruning was not applicable or was attempted and nothing was pruned
   *        (to reduce overhead in parquet group scan).
   * </ul>
   *
   * @return true if entries should be initialized with selection root, false otherwise
   */
  private boolean checkForInitializingEntriesWithSelectionRoot() {
    // TODO: at some point we should examine whether the list of entries is absolutely needed.
    return metaContext.isMetadataCacheCorrupted() || (parquetTableMetadata != null &&
        (metaContext.getPruneStatus() == PruneStatus.NOT_STARTED || metaContext.getPruneStatus() == PruneStatus.NOT_PRUNED));
  }

  /**
   * Create and return a new file selection based on reading the metadata cache file.
   *
   * This function also initializes a few of ParquetGroupScan's fields as appropriate.
   *
   * @param selection initial file selection
   * @param metaFilePath metadata cache file path
   * @return file selection read from cache
   *
   * @throws org.apache.drill.common.exceptions.UserException when the updated selection is empty,
   * this happens if the user selects an empty folder.
   */
  private FileSelection expandSelectionFromMetadataCache(FileSelection selection, Path metaFilePath) throws IOException {
    // get the metadata for the root directory by reading the metadata file
    // parquetTableMetadata contains the metadata for all files in the selection root folder, but we need to make sure
    // we only select the files that are part of selection (by setting fileSet appropriately)

    // get (and set internal field) the metadata for the directory by reading the metadata file
    FileSystem processUserFileSystem = ImpersonationUtil.createFileSystem(ImpersonationUtil.getProcessUserName(), fs.getConf());
    parquetTableMetadata = Metadata.readBlockMeta(processUserFileSystem, metaFilePath, metaContext, readerConfig);
    if (ignoreExpandingSelection(parquetTableMetadata)) {
      return selection;
    }
    if (formatConfig.areCorruptDatesAutoCorrected()) {
      ParquetReaderUtility.correctDatesInMetadataCache(this.parquetTableMetadata);
    }
    ParquetReaderUtility.transformBinaryInMetadataCache(parquetTableMetadata, readerConfig);
    List<FileStatus> fileStatuses = selection.getStatuses(fs);

    if (fileSet == null) {
      fileSet = Sets.newHashSet();
    }

    final Path first = fileStatuses.get(0).getPath();
    if (fileStatuses.size() == 1 && selection.getSelectionRoot().equals(first.toString())) {
      // we are selecting all files from selection root. Expand the file list from the cache
      for (ParquetFileMetadata file : parquetTableMetadata.getFiles()) {
        fileSet.add(file.getPath());
      }

    } else if (selection.isExpandedPartial() && !selection.hadWildcard() && cacheFileRoot != null) {
      if (selection.wasAllPartitionsPruned()) {
        // if all partitions were previously pruned, we only need to read 1 file (for the schema)
        fileSet.add(this.parquetTableMetadata.getFiles().get(0).getPath());
      } else {
        // we are here if the selection is in the expanded_partial state (i.e it has directories).  We get the
        // list of files from the metadata cache file that is present in the cacheFileRoot directory and populate
        // the fileSet. However, this is *not* the final list of files that will be scanned in execution since the
        // second phase of partition pruning will apply on the files and modify the file selection appropriately.
        for (ParquetFileMetadata file : this.parquetTableMetadata.getFiles()) {
          fileSet.add(file.getPath());
        }
      }
    } else {
      // we need to expand the files from fileStatuses
      for (FileStatus status : fileStatuses) {
        Path cacheFileRoot = status.getPath();
        if (status.isDirectory()) {
          //TODO [DRILL-4496] read the metadata cache files in parallel
          final Path metaPath = new Path(cacheFileRoot, Metadata.METADATA_FILENAME);
          final ParquetTableMetadataBase metadata = Metadata.readBlockMeta(processUserFileSystem, metaPath, metaContext, readerConfig);
          if (ignoreExpandingSelection(metadata)) {
            return selection;
          }
          for (ParquetFileMetadata file : metadata.getFiles()) {
            fileSet.add(file.getPath());
          }
        } else {
          final Path path = Path.getPathWithoutSchemeAndAuthority(cacheFileRoot);
          fileSet.add(path.toString());
        }
      }
    }

    if (fileSet.isEmpty()) {
      // no files were found, most likely we tried to query some empty sub folders
      logger.warn("The table is empty but with outdated invalid metadata cache files. Please, delete them.");
      return null;
    }

    List<String> fileNames = new ArrayList<>(fileSet);

    // when creating the file selection, set the selection root without the URI prefix
    // The reason is that the file names above have been created in the form
    // /a/b/c.parquet and the format of the selection root must match that of the file names
    // otherwise downstream operations such as partition pruning can break.
    final Path metaRootPath = Path.getPathWithoutSchemeAndAuthority(new Path(selection.getSelectionRoot()));
    this.selectionRoot = metaRootPath.toString();

    // Use the FileSelection constructor directly here instead of the FileSelection.create() method
    // because create() changes the root to include the scheme and authority; In future, if create()
    // is the preferred way to instantiate a file selection, we may need to do something different...
    // WARNING: file statuses and file names are inconsistent
    FileSelection newSelection = new FileSelection(selection.getStatuses(fs), fileNames, metaRootPath.toString(),
        cacheFileRoot, selection.wasAllPartitionsPruned());

    newSelection.setExpandedFully();
    newSelection.setMetaContext(metaContext);
    return newSelection;
  }

  private ParquetTableMetadataBase removeUnneededRowGroups(ParquetTableMetadataBase parquetTableMetadata) {
    List<ParquetFileMetadata> newFileMetadataList = new ArrayList<>();
    for (ParquetFileMetadata file : parquetTableMetadata.getFiles()) {
      if (fileSet.contains(file.getPath())) {
        newFileMetadataList.add(file);
      }
    }

    ParquetTableMetadataBase metadata = parquetTableMetadata.clone();
    metadata.assignFiles(newFileMetadataList);
    return metadata;
  }

  /**
   * If metadata is corrupted, ignore expanding selection and reset parquetTableMetadata and fileSet fields
   *
   * @param metadata parquet table metadata
   * @return true if parquet metadata is corrupted, false otherwise
   */
  private boolean ignoreExpandingSelection(ParquetTableMetadataBase metadata) {
    if (metadata == null || metaContext.isMetadataCacheCorrupted()) {
      logger.debug("Selection can't be expanded since metadata file is corrupted or metadata version is not supported");
      this.parquetTableMetadata = null;
      this.fileSet = null;
      return true;
    }
    return false;
  }
  // private methods block end

}
