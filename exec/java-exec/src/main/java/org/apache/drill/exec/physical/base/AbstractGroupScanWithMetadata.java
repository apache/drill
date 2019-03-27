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
package org.apache.drill.exec.physical.base;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.commons.collections.CollectionUtils;
import org.apache.drill.common.types.Types;
import org.apache.drill.common.expression.ErrorCollector;
import org.apache.drill.common.expression.ErrorCollectorImpl;
import org.apache.drill.common.expression.ExpressionStringBuilder;
import org.apache.drill.common.expression.LogicalExpression;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.common.expression.ValueExpressions;
import org.apache.drill.common.types.TypeProtos;
import org.apache.drill.exec.compile.sig.ConstantExpressionIdentifier;
import org.apache.drill.exec.expr.ExpressionTreeMaterializer;
import org.apache.drill.exec.expr.fn.FunctionImplementationRegistry;
import org.apache.drill.exec.expr.stat.RowsMatch;
import org.apache.drill.exec.ops.OptimizerRulesContext;
import org.apache.drill.exec.ops.UdfUtilities;
import org.apache.drill.exec.planner.physical.PlannerSettings;
import org.apache.drill.exec.record.MaterializedField;
import org.apache.drill.exec.record.metadata.ColumnMetadata;
import org.apache.drill.exec.record.metadata.SchemaPathUtils;
import org.apache.drill.exec.record.metadata.TupleMetadata;
import org.apache.drill.exec.record.metadata.TupleSchema;
import org.apache.drill.exec.server.options.OptionManager;
import org.apache.drill.exec.store.ColumnExplorer;
import org.apache.drill.exec.store.dfs.FileSelection;
import org.apache.drill.exec.store.parquet.FilterEvaluatorUtils;
import org.apache.drill.exec.store.parquet.ParquetTableMetadataUtils;
import org.apache.drill.metastore.BaseMetadata;
import org.apache.drill.metastore.ColumnStatistics;
import org.apache.drill.metastore.ColumnStatisticsKind;
import org.apache.drill.metastore.FileMetadata;
import org.apache.drill.metastore.LocationProvider;
import org.apache.drill.metastore.PartitionMetadata;
import org.apache.drill.metastore.TableMetadata;
import org.apache.drill.metastore.TableStatisticsKind;
import org.apache.drill.exec.expr.FilterBuilder;
import org.apache.drill.exec.expr.FilterPredicate;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

// import static org.apache.drill.exec.ExecConstants.HIVE_OPTIMIZE_SCAN_WITH_NATIVE_READERS;
import static org.apache.drill.exec.ExecConstants.SKIP_RUNTIME_ROWGROUP_PRUNING_KEY;

/**
 * Represents table group scan with metadata usage.
 */
public abstract class AbstractGroupScanWithMetadata extends AbstractFileGroupScan {

  protected TableMetadataProvider metadataProvider;

  // table metadata info
  protected TableMetadata tableMetadata;

  // partition metadata info: mixed partition values for all partition keys in the same list
  protected List<PartitionMetadata> partitions;

  protected List<SchemaPath> partitionColumns;
  protected LogicalExpression filter;
  protected List<SchemaPath> columns;

  protected Map<Path, FileMetadata> files;

  // set of the files to be handled
  protected Set<Path> fileSet;

  // whether all files, partitions or row groups of this group scan fully match the filter
  protected boolean matchAllMetadata = false;

  protected AbstractGroupScanWithMetadata(String userName, List<SchemaPath> columns, LogicalExpression filter) {
    super(userName);
    this.columns = columns;
    this.filter = filter;
  }

  protected AbstractGroupScanWithMetadata(AbstractGroupScanWithMetadata that) {
    super(that.getUserName());
    this.columns = that.columns;
    this.filter = that.filter;
    this.matchAllMetadata = that.matchAllMetadata;

    this.metadataProvider = that.metadataProvider;
    this.tableMetadata = that.tableMetadata;
    this.partitionColumns = that.partitionColumns;
    this.partitions = that.partitions;
    this.files = that.files;

    this.fileSet = that.fileSet == null ? null : new HashSet<>(that.fileSet);
  }

  @JsonProperty("columns")
  @Override
  public List<SchemaPath> getColumns() {
    return columns;
  }

  @JsonIgnore
  @Override
  public Collection<Path> getFiles() {
    return fileSet;
  }

  @Override
  public boolean hasFiles() {
    return true;
  }

  @JsonIgnore
  public boolean isMatchAllMetadata() {
    return matchAllMetadata;
  }

  /**
   * Return column value count for the specified column.
   * If does not contain such column, return 0.
   * Is used when applying convert to direct scan rule.
   *
   * @param column column schema path
   * @return column value count
   */
  @Override
  public long getColumnValueCount(SchemaPath column) {
    long tableRowCount = (long) TableStatisticsKind.ROW_COUNT.getValue(getTableMetadata());
    ColumnStatistics columnStats = getTableMetadata().getColumnStatistics(column);
    long colNulls;
    if (columnStats != null) {
      Long nulls = (Long) columnStats.getStatistic(ColumnStatisticsKind.NULLS_COUNT);
      colNulls = nulls != null ? nulls : GroupScan.NO_COLUMN_STATS;
    } else {
      return 0;
    }
    return GroupScan.NO_COLUMN_STATS == tableRowCount
        || GroupScan.NO_COLUMN_STATS == colNulls
        ? GroupScan.NO_COLUMN_STATS : tableRowCount - colNulls;
  }

  @Override
  public String getDigest() {
    return toString();
  }

  @Override
  public ScanStats getScanStats() {
    int columnCount = columns == null ? 20 : columns.size();
    double rowCount = (long) TableStatisticsKind.ROW_COUNT.getValue(getTableMetadata());

    ScanStats scanStats = new ScanStats(ScanStats.GroupScanProperty.EXACT_ROW_COUNT, rowCount, 1, rowCount * columnCount);
    logger.trace("Drill parquet scan statistics: {}", scanStats);
    return scanStats;
  }

  // filter push down methods block start
  @JsonProperty("filter")
  @Override
  public LogicalExpression getFilter() {
    return filter;
  }

  @Override
  public TableMetadataProvider getMetadataProvider() {
    return metadataProvider;
  }

  public void setFilter(LogicalExpression filter) {
    this.filter = filter;
  }

  /**
   *  Set the filter - thus enabling runtime rowgroup pruning
   *  The runtime pruning can be disabled with an option.
   * @param filterExpr The filter to be used at runtime to match with rowgroups' footers
   * @param optimizerContext The context for the options
   */
  public void setFilterForRuntime(LogicalExpression filterExpr, OptimizerRulesContext optimizerContext) {
    OptionManager options = optimizerContext.getPlannerSettings().getOptions();
    boolean skipRuntimePruning = options.getBoolean(SKIP_RUNTIME_ROWGROUP_PRUNING_KEY); // if option is set to disable runtime pruning
    if ( ! skipRuntimePruning ) { setFilter(filterExpr); }
  }

  @Override
  public AbstractGroupScanWithMetadata applyFilter(LogicalExpression filterExpr, UdfUtilities udfUtilities,
      FunctionImplementationRegistry functionImplementationRegistry, OptionManager optionManager) {

    // Builds filter for pruning. If filter cannot be built, null should be returned.
    FilterPredicate filterPredicate =
            getFilterPredicate(filterExpr, udfUtilities, functionImplementationRegistry, optionManager, true);
    if (filterPredicate == null) {
      logger.debug("FilterPredicate cannot be built.");
      return null;
    }

    final Set<SchemaPath> schemaPathsInExpr =
        filterExpr.accept(new FilterEvaluatorUtils.FieldReferenceFinder(), null);

    GroupScanWithMetadataFilterer filteredMetadata = getFilterer().getFiltered(optionManager, filterPredicate, schemaPathsInExpr);

    if (!getFilesMetadata().isEmpty()) {
      if (!filteredMetadata.getFiles().isEmpty() && getFilesMetadata().size() == filteredMetadata.getFiles().size()) {
        // There is no reduction of files, but filter may be omitted.
        logger.debug("applyFilter() does not have any pruning since GroupScan fully matches filter");
        matchAllMetadata = filteredMetadata.isMatchAllMetadata();
        return null;
      }
    } else if (!getPartitionsMetadata().isEmpty()) {
      // for the case when files metadata wasn't created, check partition metadata
      if (!filteredMetadata.getPartitions().isEmpty() && getPartitionsMetadata().size() == filteredMetadata.getPartitions().size()) {
        // There is no reduction of partitions, but filter may be omitted.
        logger.debug("applyFilter() does not have any pruning since GroupScan fully matches filter");
        matchAllMetadata = filteredMetadata.isMatchAllMetadata();
        return null;
      }
    } else if (getTableMetadata() != null) {
      // There is no reduction of files or partitions, but filter may be omitted.
      logger.debug("applyFilter() does not have any pruning since GroupScan fully matches filter");
      matchAllMetadata = filteredMetadata.isMatchAllMetadata();
      return null;
    }

    if (!filteredMetadata.isMatchAllMetadata()
      // filter returns empty result using table metadata
      && ((filteredMetadata.getTableMetadata() == null && getTableMetadata() != null)
          // all partitions are pruned if partition metadata is available
          || filteredMetadata.getPartitions().isEmpty() && !getPartitionsMetadata().isEmpty()
          // all files are pruned if file metadata is available
          || filteredMetadata.getFiles().isEmpty() && !getFilesMetadata().isEmpty())) {
      if (getFilesMetadata().size() == 1) {
        // For the case when group scan has single file and it was filtered,
        // no need to create new group scan with the same file.
        return null;
      }
      logger.debug("All files have been filtered out. Add back one to get schema from scanner");
      Map<Path, FileMetadata> filesMap = getNextOrEmpty(getFilesMetadata().values()).stream()
          .collect(Collectors.toMap(FileMetadata::getLocation, Function.identity()));

      filteredMetadata.withTable(getTableMetadata())
          .withPartitions(getNextOrEmpty(getPartitionsMetadata()))
          .withFiles(filesMap)
          .withMatching(false);
    }

    return filteredMetadata.build();
  }

  /**
   * Returns list with the first element of input list or empty list if input one was empty.
   *
   * @param inputList the source of the first element
   * @param <T>       type of values in the list
   * @return list with the first element of input list
   */
  protected <T> List<T> getNextOrEmpty(Collection<T> inputList) {
    return CollectionUtils.isNotEmpty(inputList) ? Collections.singletonList(inputList.iterator().next()) : Collections.emptyList();
  }

  /**
   * Returns holder for metadata values which provides API to filter metadata
   * and build new group scan instance using filtered metadata.
   */
  protected abstract GroupScanWithMetadataFilterer getFilterer();

  public FilterPredicate getFilterPredicate(LogicalExpression filterExpr,
                                                   UdfUtilities udfUtilities,
                                                   FunctionImplementationRegistry functionImplementationRegistry,
                                                   OptionManager optionManager,
                                                   boolean omitUnsupportedExprs) {
    return getFilterPredicate(filterExpr, udfUtilities, functionImplementationRegistry, optionManager,
            omitUnsupportedExprs, supportsFileImplicitColumns(), (TupleSchema) getTableMetadata().getSchema());
  }
  /**
   * Returns parquet filter predicate built from specified {@code filterExpr}.
   *
   * @param filterExpr                     filter expression to build
   * @param udfUtilities                   udf utilities
   * @param functionImplementationRegistry context to find drill function holder
   * @param omitUnsupportedExprs           whether expressions which cannot be converted
   *                                       may be omitted from the resulting expression
   * @return parquet filter predicate
   */
  public static FilterPredicate getFilterPredicate(LogicalExpression filterExpr,
                                            UdfUtilities udfUtilities,
                                            FunctionImplementationRegistry functionImplementationRegistry,
                                            OptionManager optionManager,
                                            boolean omitUnsupportedExprs,
                                            boolean supportsFileImplicitColumns,
                                            TupleSchema tupleSchema) {
    TupleMetadata types = tupleSchema.copy();

    Set<SchemaPath> schemaPathsInExpr = filterExpr.accept(new FilterEvaluatorUtils.FieldReferenceFinder(), null);

    // adds implicit or partition columns if they weren't added before.
    if (supportsFileImplicitColumns) {
      for (SchemaPath schemaPath : schemaPathsInExpr) {
        if (isImplicitOrPartCol(schemaPath, optionManager) && SchemaPathUtils.getColumnMetadata(schemaPath, types) == null) {
          types.add(MaterializedField.create(schemaPath.getRootSegmentPath(), Types.required(TypeProtos.MinorType.VARCHAR)));
        }
      }
    }

    ErrorCollector errorCollector = new ErrorCollectorImpl();
    LogicalExpression materializedFilter = ExpressionTreeMaterializer.materializeFilterExpr(
        filterExpr, types, errorCollector, functionImplementationRegistry);

    if (errorCollector.hasErrors()) {
      logger.error("{} error(s) encountered when materialize filter expression : {}",
        errorCollector.getErrorCount(), errorCollector.toErrorString());
      return null;
    }
    logger.debug("materializedFilter : {}", ExpressionStringBuilder.toString(materializedFilter));

    Set<LogicalExpression> constantBoundaries = ConstantExpressionIdentifier.getConstantExpressionSet(materializedFilter);
    return FilterBuilder.buildFilterPredicate(materializedFilter, constantBoundaries, udfUtilities, omitUnsupportedExprs);
  }

  @JsonIgnore
  public TupleSchema getSchema() {
    // creates a copy of TupleMetadata from tableMetadata
    TupleSchema tuple = new TupleSchema();
    for (ColumnMetadata md : getTableMetadata().getSchema()) {
      tuple.addColumn(md.copy());
    }
    return tuple;
  }

  // limit push down methods start
  @Override
  public boolean supportsLimitPushdown() {
    return true;
  }

  @Override
  public GroupScan applyLimit(int maxRecords) {
    maxRecords = Math.max(maxRecords, 1); // Make sure it request at least 1 row -> 1 file.
    GroupScanWithMetadataFilterer prunedMetadata = getFilterer();
    if (getTableMetadata() != null) {
      long tableRowCount = (long) TableStatisticsKind.ROW_COUNT.getValue(getTableMetadata());
      if (tableRowCount == NO_COLUMN_STATS || tableRowCount <= maxRecords) {
        logger.debug("limit push down does not apply, since total number of rows [{}] is less or equal to the required [{}].",
            tableRowCount, maxRecords);
        return null;
      }
    }
    // Calculate number of files to read based on maxRecords and update
    // number of records to read for each of those files.
    List<FileMetadata> qualifiedFiles = limitMetadata(getFilesMetadata().values(), maxRecords);

    // some files does not have set row count, do not do files pruning
    if (qualifiedFiles == null || qualifiedFiles.size() == getFilesMetadata().size()) {
      logger.debug("limit push down does not apply, since number of files was not reduced.");
      return null;
    }

    Map<Path, FileMetadata> filesMap = qualifiedFiles.stream()
        .collect(Collectors.toMap(FileMetadata::getLocation, Function.identity()));

    return prunedMetadata
        .withTable(getTableMetadata())
        .withPartitions(getPartitionsMetadata())
        .withFiles(filesMap)
        .withMatching(matchAllMetadata)
        .build();
  }

  /**
   * Removes metadata which does not belong to any of partitions in metadata list.
   *
   * @param metadataToPrune           list of metadata which should be pruned
   * @param filteredPartitionMetadata list of partition metadata which was pruned
   * @param <T>                       type of metadata to filter
   * @return list with metadata which belongs to pruned partitions
   */
  protected static <T extends BaseMetadata & LocationProvider> Map<Path, T> pruneForPartitions(Map<Path, T> metadataToPrune, List<PartitionMetadata> filteredPartitionMetadata) {
    Map<Path, T> prunedFiles = new LinkedHashMap<>();
    if (metadataToPrune != null) {
      metadataToPrune.forEach((path, metadata) -> {
        for (PartitionMetadata filteredPartition : filteredPartitionMetadata) {
          if (filteredPartition.getLocations().contains(path)) {
            prunedFiles.put(path, metadata);
            break;
          }
        }
      });
    }

    return prunedFiles;
  }

  /**
   * Prunes specified metadata list and leaves minimum metadata instances count with general rows number
   * which is not less than specified {@code maxRecords}.
   *
   * @param metadataList list of metadata to prune
   * @param maxRecords   rows number to leave
   * @param <T>          type of metadata to prune
   * @return pruned metadata list
   */
  protected <T extends BaseMetadata> List<T> limitMetadata(Collection<T> metadataList, int maxRecords) {
    List<T> qualifiedMetadata = new ArrayList<>();
    int currentRowCount = 0;
    for (T metadata : metadataList) {
      long rowCount = (long) TableStatisticsKind.ROW_COUNT.getValue(metadata);
      if (rowCount == NO_COLUMN_STATS) {
        return null;
      } else if (currentRowCount + rowCount <= maxRecords) {
        currentRowCount += rowCount;
        qualifiedMetadata.add(metadata);
        continue;
      } else if (currentRowCount < maxRecords) {
        qualifiedMetadata.add(metadata);
      }
      break;
    }
    return qualifiedMetadata;
  }
  // limit push down methods end

  // partition pruning methods start
  @Override
  public List<SchemaPath> getPartitionColumns() {
    return partitionColumns != null ? partitionColumns : new ArrayList<>();
  }

  @JsonIgnore
  public TypeProtos.MajorType getTypeForColumn(SchemaPath schemaPath) {
    ColumnMetadata columnMetadata = SchemaPathUtils.getColumnMetadata(schemaPath, getTableMetadata().getSchema());
    return columnMetadata != null ? columnMetadata.majorType() : null;
  }

  @JsonIgnore
  public <T> T getPartitionValue(Path path, SchemaPath column, Class<T> clazz) {
    return getPartitionsMetadata().stream()
        .filter(partition -> partition.getColumn().equals(column) && partition.getLocations().contains(path))
        .findAny()
        .map(metadata -> clazz.cast(metadata.getColumnsStatistics().get(column).getStatistic(ColumnStatisticsKind.MAX_VALUE)))
        .orElse(null);
  }

  @JsonIgnore
  public Set<Path> getFileSet() {
    return fileSet;
  }
  // partition pruning methods end

  // helper method used for partition pruning and filter push down
  @Override
  public void modifyFileSelection(FileSelection selection) {
    fileSet = new HashSet<>(selection.getFiles());
  }

  // protected methods block
  protected void init() throws IOException {
    if (fileSet == null && getFilesMetadata() != null) {
      fileSet = getFilesMetadata().keySet();
    }
  }

  protected String getFilterString() {
    return filter == null || filter.equals(ValueExpressions.BooleanExpression.TRUE) ?
      "" : ExpressionStringBuilder.toString(this.filter);
  }

  protected abstract boolean supportsFileImplicitColumns();
  protected abstract List<String> getPartitionValues(LocationProvider locationProvider);

  public static boolean isImplicitOrPartCol(SchemaPath schemaPath, OptionManager optionManager) {
    Set<String> implicitColNames = ColumnExplorer.initImplicitFileColumns(optionManager).keySet();
    return ColumnExplorer.isPartitionColumn(optionManager, schemaPath) || implicitColNames.contains(schemaPath.getRootSegmentPath());
  }

  // protected methods for internal usage
  protected Map<Path, FileMetadata> getFilesMetadata() {
    if (files == null) {
      files = ((ParquetMetadataProvider) metadataProvider).getFilesMetadataMap();
    }
    return files;
  }

  @Override
  public TableMetadata getTableMetadata() {
    if (tableMetadata == null) {
      tableMetadata = metadataProvider.getTableMetadata();
    }
    return tableMetadata;
  }

  protected List<PartitionMetadata> getPartitionsMetadata() {
    if (partitions == null) {
      partitions = metadataProvider.getPartitionsMetadata();
    }
    return partitions;
  }

  /**
   * This class is responsible for filtering different metadata levels.
   */
  protected abstract static class GroupScanWithMetadataFilterer {
    protected final AbstractGroupScanWithMetadata source;

    protected boolean matchAllMetadata = false;

    protected TableMetadata tableMetadata;
    protected List<PartitionMetadata> partitions = Collections.emptyList();
    protected Map<Path, FileMetadata> files = Collections.emptyMap();

    // for the case when filtering is possible for partitions, but files count exceeds
    // PARQUET_ROWGROUP_FILTER_PUSHDOWN_PLANNING_THRESHOLD, new group scan with at least filtered partitions
    // and files which belongs to that partitions may be returned
    protected MetadataLevel overflowLevel = MetadataLevel.NONE;

    public GroupScanWithMetadataFilterer(AbstractGroupScanWithMetadata source) {
      this.source = source;
    }

    /**
     * Constructs required implementation of {@link AbstractGroupScanWithMetadata} with filtered metadata.
     *
     * @return implementation of {@link AbstractGroupScanWithMetadata} with filtered metadata
     */
    public abstract AbstractGroupScanWithMetadata build();

    public GroupScanWithMetadataFilterer withTable(TableMetadata tableMetadata) {
      this.tableMetadata = tableMetadata;
      return this;
    }

    public GroupScanWithMetadataFilterer withPartitions(List<PartitionMetadata> partitions) {
      this.partitions = partitions;
      return this;
    }

    public GroupScanWithMetadataFilterer withFiles(Map<Path, FileMetadata> files) {
      this.files = files;
      return this;
    }

    public GroupScanWithMetadataFilterer withMatching(boolean matchAllMetadata) {
      this.matchAllMetadata = matchAllMetadata;
      return this;
    }

    public GroupScanWithMetadataFilterer withOverflow(MetadataLevel overflowLevel) {
      this.overflowLevel = overflowLevel;
      return this;
    }

    public boolean isMatchAllMetadata() {
      return matchAllMetadata;
    }

    public TableMetadata getTableMetadata() {
      return tableMetadata;
    }

    public List<PartitionMetadata> getPartitions() {
      return partitions;
    }

    public Map<Path, FileMetadata> getFiles() {
      return files;
    }

    public MetadataLevel getOverflowLevel() {
      return overflowLevel;
    }

    /**
     * Produces filtering of metadata and returns {@link GroupScanWithMetadataFilterer}
     * to construct resulting group scan.
     *
     * @param optionManager     option manager
     * @param filterPredicate   filter expression
     * @param schemaPathsInExpr columns used in filter expression
     * @return this instance with filtered metadata
     */
    protected GroupScanWithMetadataFilterer getFiltered(OptionManager optionManager,
                                                        FilterPredicate filterPredicate,
                                                        Set<SchemaPath> schemaPathsInExpr) {
      if (source.getTableMetadata() != null) {
        filterTableMetadata(filterPredicate, schemaPathsInExpr);
      }

      if (source.getPartitionsMetadata() != null) {
        filterPartitionMetadata(optionManager, filterPredicate, schemaPathsInExpr);
      }

      if (source.getFilesMetadata() != null) {
        filterFileMetadata(optionManager, filterPredicate, schemaPathsInExpr);
      }
      return this;
    }

    /**
     * Produces filtering of metadata at table level.
     *
     * @param filterPredicate   filter expression
     * @param schemaPathsInExpr columns used in filter expression
     */
    protected void filterTableMetadata(FilterPredicate filterPredicate, Set<SchemaPath> schemaPathsInExpr) {
      // Filters table metadata. If resulting list is empty, should be used single minimum entity of metadata.
      // If table matches fully, nothing is pruned and pruning of underlying metadata is stopped.
      matchAllMetadata = true;
      List<TableMetadata> filteredTableMetadata = filterAndGetMetadata(schemaPathsInExpr,
          Collections.singletonList(source.getTableMetadata()), filterPredicate, null);
      if (CollectionUtils.isNotEmpty(filteredTableMetadata)) {
        this.tableMetadata = filteredTableMetadata.get(0);
      }
    }

    /**
     * Produces filtering of metadata at partition level.
     *
     * @param optionManager     option manager
     * @param filterPredicate   filter expression
     * @param schemaPathsInExpr columns used in filter expression
     */
    protected void filterPartitionMetadata(OptionManager optionManager,
                                           FilterPredicate filterPredicate,
                                           Set<SchemaPath> schemaPathsInExpr) {
      if (!matchAllMetadata) {
        if (!source.getPartitionsMetadata().isEmpty()) {
          if (source.getPartitionsMetadata().size() <= optionManager.getOption(
            PlannerSettings.PARQUET_ROWGROUP_FILTER_PUSHDOWN_PLANNING_THRESHOLD)) {
            matchAllMetadata = true;
            partitions = filterAndGetMetadata(schemaPathsInExpr, source.getPartitionsMetadata(), filterPredicate, optionManager);
          } else {
            overflowLevel = MetadataLevel.PARTITION;
          }
        }
      } else {
        partitions = source.getPartitionsMetadata();
      }
    }

    /**
     * Produces filtering of metadata at file level.
     *
     * @param optionManager     option manager
     * @param filterPredicate   filter expression
     * @param schemaPathsInExpr columns used in filter expression
     */
    protected void filterFileMetadata(OptionManager optionManager,
                                      FilterPredicate filterPredicate,
                                      Set<SchemaPath> schemaPathsInExpr) {
      Map<Path, FileMetadata> prunedFiles;
      if (CollectionUtils.isEmpty(source.getPartitionsMetadata())
          || source.getPartitionsMetadata().size() == getPartitions().size()) {
        // no partition pruning happened, no need to prune initial files list
        prunedFiles = source.getFilesMetadata();
      } else {
        // prunes files to leave only files which are contained by pruned partitions
        prunedFiles = pruneForPartitions(source.getFilesMetadata(), partitions);
      }

      if (matchAllMetadata) {
        files = prunedFiles;
        return;
      }

      // Stop files pruning for the case:
      //    -  # of files is beyond PARQUET_ROWGROUP_FILTER_PUSHDOWN_PLANNING_THRESHOLD.
      if (prunedFiles.size() <= optionManager.getOption(
        PlannerSettings.PARQUET_ROWGROUP_FILTER_PUSHDOWN_PLANNING_THRESHOLD)) {

        matchAllMetadata = true;
        files = filterAndGetMetadata(schemaPathsInExpr, prunedFiles.values(), filterPredicate, optionManager).stream()
            .collect(Collectors.toMap(FileMetadata::getLocation, Function.identity()));

      } else {
        matchAllMetadata = false;
        files = prunedFiles;
        overflowLevel = MetadataLevel.FILE;
      }
    }

    /**
     * Produces filtering of specified metadata using specified filter expression and returns filtered metadata.
     *
     * @param schemaPathsInExpr columns used in filter expression
     * @param metadataList      metadata to filter
     * @param filterPredicate   filter expression
     * @param optionManager     option manager
     * @param <T>               type of metadata to filter
     * @return filtered metadata
     */
    public <T extends BaseMetadata> List<T> filterAndGetMetadata(Set<SchemaPath> schemaPathsInExpr,
                                                                 Iterable<T> metadataList,
                                                                 FilterPredicate filterPredicate,
                                                                 OptionManager optionManager) {
      List<T> qualifiedFiles = new ArrayList<>();

      for (T metadata : metadataList) {
        Map<SchemaPath, ColumnStatistics> columnsStatistics = metadata.getColumnsStatistics();

        // adds partition (dir) column statistics if it may be used during filter evaluation
        if (metadata instanceof LocationProvider && optionManager != null) {
          LocationProvider locationProvider = (LocationProvider) metadata;
          columnsStatistics = ParquetTableMetadataUtils.addImplicitColumnsStatistics(columnsStatistics,
              source.columns, source.getPartitionValues(locationProvider), optionManager,
              locationProvider.getLocation(), source.supportsFileImplicitColumns());
        }

        RowsMatch match = FilterEvaluatorUtils.matches(filterPredicate,
            columnsStatistics, (long) metadata.getStatistic(TableStatisticsKind.ROW_COUNT),
            metadata.getSchema(), schemaPathsInExpr);
        if (match == RowsMatch.NONE) {
          continue; // No file comply to the filter => drop the file
        }
        if (matchAllMetadata) {
          matchAllMetadata = match == RowsMatch.ALL;
        }
        qualifiedFiles.add(metadata);
      }
      if (qualifiedFiles.isEmpty()) {
        matchAllMetadata = false;
      }
      return qualifiedFiles;
    }
  }

  public enum MetadataLevel {
    TABLE, PARTITION, FILE, ROW_GROUP, NONE
  }
}
