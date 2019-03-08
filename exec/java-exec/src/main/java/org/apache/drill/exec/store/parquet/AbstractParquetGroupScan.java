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

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.commons.collections.CollectionUtils;
import org.apache.drill.common.expression.ExpressionStringBuilder;
import org.apache.drill.exec.physical.base.AbstractGroupScanWithMetadata;
import org.apache.drill.exec.physical.base.ParquetMetadataProvider;
import org.apache.drill.exec.record.metadata.TupleMetadata;
import org.apache.drill.metastore.BaseMetadata;
import org.apache.drill.metastore.LocationProvider;
import org.apache.drill.metastore.PartitionMetadata;
import org.apache.drill.metastore.TableStatisticsKind;
import org.apache.drill.exec.expr.ExactStatisticsConstants;
import org.apache.drill.shaded.guava.com.google.common.base.Preconditions;
import org.apache.drill.shaded.guava.com.google.common.collect.ArrayListMultimap;
import org.apache.drill.shaded.guava.com.google.common.collect.LinkedListMultimap;
import org.apache.drill.shaded.guava.com.google.common.collect.ListMultimap;
import org.apache.drill.common.expression.LogicalExpression;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.exec.expr.fn.FunctionImplementationRegistry;
import org.apache.drill.exec.ops.UdfUtilities;
import org.apache.drill.exec.physical.EndpointAffinity;
import org.apache.drill.exec.physical.base.GroupScan;
import org.apache.drill.exec.planner.physical.PlannerSettings;
import org.apache.drill.exec.proto.CoordinationProtos;
import org.apache.drill.exec.server.options.OptionManager;
import org.apache.drill.exec.store.dfs.FileSelection;
import org.apache.drill.exec.store.dfs.ReadEntryWithPath;
import org.apache.drill.exec.store.schedule.AffinityCreator;
import org.apache.drill.exec.store.schedule.AssignmentCreator;
import org.apache.drill.exec.store.schedule.EndpointByteMap;
import org.apache.drill.exec.store.schedule.EndpointByteMapImpl;
import org.apache.drill.metastore.FileMetadata;
import org.apache.drill.metastore.RowGroupMetadata;
import org.apache.drill.exec.expr.FilterPredicate;
import org.apache.drill.shaded.guava.com.google.common.collect.Multimap;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

public abstract class AbstractParquetGroupScan extends AbstractGroupScanWithMetadata {

  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(AbstractParquetGroupScan.class);

  protected List<ReadEntryWithPath> entries;
  protected Multimap<Path, RowGroupMetadata> rowGroups;

  protected ListMultimap<Integer, RowGroupInfo> mappings;
  protected ParquetReaderConfig readerConfig;

  private List<EndpointAffinity> endpointAffinities;
  // used for applying assignments for incoming endpoints
  private List<RowGroupInfo> rowGroupInfos;

  protected AbstractParquetGroupScan(String userName,
                                     List<SchemaPath> columns,
                                     List<ReadEntryWithPath> entries,
                                     ParquetReaderConfig readerConfig,
                                     LogicalExpression filter) {
    super(userName, columns, filter);
    this.entries = entries;
    this.readerConfig = readerConfig == null ? ParquetReaderConfig.getDefaultInstance() : readerConfig;
  }

  // immutable copy constructor
  protected AbstractParquetGroupScan(AbstractParquetGroupScan that) {
    super(that);

    this.rowGroups = that.rowGroups;

    this.endpointAffinities = that.endpointAffinities == null ? null : new ArrayList<>(that.endpointAffinities);
    this.mappings = that.mappings == null ? null : ArrayListMultimap.create(that.mappings);

    this.entries = that.entries == null ? null : new ArrayList<>(that.entries);
    this.readerConfig = that.readerConfig;

  }

  @JsonProperty
  public List<ReadEntryWithPath> getEntries() {
    return entries;
  }

  @JsonProperty("readerConfig")
  @JsonInclude(JsonInclude.Include.NON_NULL)
  // do not serialize reader config if it contains all default values
  public ParquetReaderConfig getReaderConfigForSerialization() {
    return ParquetReaderConfig.getDefaultInstance().equals(readerConfig) ? null : readerConfig;
  }

  @JsonIgnore
  public ParquetReaderConfig getReaderConfig() {
    return readerConfig;
  }

  @Override
  public boolean canPushdownProjects(List<SchemaPath> columns) {
    return true;
  }

  /**
   * Calculates the affinity each endpoint has for this scan,
   * by adding up the affinity each endpoint has for each rowGroup.
   *
   * @return a list of EndpointAffinity objects
   */
  @Override
  public List<EndpointAffinity> getOperatorAffinity() {
    return endpointAffinities;
  }

  @Override
  public void applyAssignments(List<CoordinationProtos.DrillbitEndpoint> incomingEndpoints) {
    this.mappings = AssignmentCreator.getMappings(incomingEndpoints, getRowGroupInfos());
  }

  private List<RowGroupInfo> getRowGroupInfos() {
    if (rowGroupInfos == null) {
      Map<String, CoordinationProtos.DrillbitEndpoint> hostEndpointMap = new HashMap<>();

      for (CoordinationProtos.DrillbitEndpoint endpoint : getDrillbits()) {
        hostEndpointMap.put(endpoint.getAddress(), endpoint);
      }

      rowGroupInfos = new ArrayList<>();
      for (RowGroupMetadata rowGroupMetadata : getRowGroupsMetadata().values()) {
        RowGroupInfo rowGroupInfo = new RowGroupInfo(rowGroupMetadata.getLocation(),
            (long) rowGroupMetadata.getStatistic(() -> ExactStatisticsConstants.START),
            (long) rowGroupMetadata.getStatistic(() -> ExactStatisticsConstants.LENGTH),
            rowGroupMetadata.getRowGroupIndex(),
            (long) rowGroupMetadata.getStatistic(TableStatisticsKind.ROW_COUNT));
        rowGroupInfo.setNumRecordsToRead(rowGroupInfo.getRowCount());

        EndpointByteMap endpointByteMap = new EndpointByteMapImpl();
        for (String host : rowGroupMetadata.getHostAffinity().keySet()) {
          if (hostEndpointMap.containsKey(host)) {
            endpointByteMap.add(hostEndpointMap.get(host),
              (long) (rowGroupMetadata.getHostAffinity().get(host) * (long) rowGroupMetadata.getStatistic(() -> ExactStatisticsConstants.LENGTH)));
          }
        }

        rowGroupInfo.setEndpointByteMap(endpointByteMap);

        rowGroupInfos.add(rowGroupInfo);
      }
    }
    return rowGroupInfos;
  }

  @Override
  public int getMaxParallelizationWidth() {
    if (!getRowGroupsMetadata().isEmpty()) {
      return getRowGroupsMetadata().size();
    } else {
      if (!getFilesMetadata().isEmpty()) {
        return getFilesMetadata().size();
      } else {
        return !getPartitionsMetadata().isEmpty() ? getPartitionsMetadata().size() : 1;
      }
    }
  }

  protected List<RowGroupReadEntry> getReadEntries(int minorFragmentId) {
    assert minorFragmentId < mappings.size() : String
        .format("Mappings length [%d] should be longer than minor fragment id [%d] but it isn't.",
            mappings.size(), minorFragmentId);

    List<RowGroupInfo> rowGroupsForMinor = mappings.get(minorFragmentId);

    Preconditions.checkArgument(!rowGroupsForMinor.isEmpty(),
        String.format("MinorFragmentId %d has no read entries assigned", minorFragmentId));

    List<RowGroupReadEntry> readEntries = new ArrayList<>();
    for (RowGroupInfo rgi : rowGroupsForMinor) {
      RowGroupReadEntry entry = new RowGroupReadEntry(rgi.getPath(), rgi.getStart(),
          rgi.getLength(), rgi.getRowGroupIndex(),
          rgi.getNumRecordsToRead());
      readEntries.add(entry);
    }
    return readEntries;
  }

  @Override
  public AbstractGroupScanWithMetadata applyFilter(LogicalExpression filterExpr, UdfUtilities udfUtilities,
      FunctionImplementationRegistry functionImplementationRegistry, OptionManager optionManager) {
    // Builds filter for pruning. If filter cannot be built, null should be returned.
    FilterPredicate filterPredicate = getFilterPredicate(filterExpr, udfUtilities, functionImplementationRegistry, optionManager, true);
    if (filterPredicate == null) {
      logger.debug("FilterPredicate cannot be built.");
      return null;
    }

    Set<SchemaPath> schemaPathsInExpr =
        filterExpr.accept(new FilterEvaluatorUtils.FieldReferenceFinder(), null);

    RowGroupScanFilterer builder = getFilterer().getFiltered(optionManager, filterPredicate, schemaPathsInExpr);

    // checks whether metadata for specific level was available and there was no reduction of metadata
    if (!getRowGroupsMetadata().isEmpty()) {
      if (!builder.getRowGroups().isEmpty() && getRowGroupsMetadata().size() == builder.getRowGroups().size()) {
        // There is no reduction of files
        logger.debug("applyFilter() does not have any pruning");
        matchAllMetadata = builder.isMatchAllMetadata();
        return null;
      }
    } else if (!getFilesMetadata().isEmpty()) {
      if (!builder.getFiles().isEmpty() && getFilesMetadata().size() == builder.getFiles().size()) {
        // There is no reduction of files
        logger.debug("applyFilter() does not have any pruning");
        matchAllMetadata = builder.isMatchAllMetadata();
        return null;
      }
    } else if (!getPartitionsMetadata().isEmpty()) {
      if (!builder.getPartitions().isEmpty() && getPartitionsMetadata().size() == builder.getPartitions().size()) {
        // There is no reduction of partitions
        logger.debug("applyFilter() does not have any pruning ");
        matchAllMetadata = builder.isMatchAllMetadata();
        return null;
      }
    } else if (getTableMetadata() != null) {
      // There is no reduction
      logger.debug("applyFilter() does not have any pruning");
      matchAllMetadata = builder.isMatchAllMetadata();
      return null;
    }

    if (!builder.isMatchAllMetadata()
        // filter returns empty result using table metadata
        && ((builder.getTableMetadata() == null && getTableMetadata() != null)
            // all partitions pruned if partition metadata is available
            || builder.getPartitions().isEmpty() && !getPartitionsMetadata().isEmpty())
            // all files are pruned if file metadata is available
            || builder.getFiles().isEmpty() && !getFilesMetadata().isEmpty()
            // all row groups are pruned if row group metadata is available
            || builder.getRowGroups().isEmpty() && !getRowGroupsMetadata().isEmpty()) {
      if (getRowGroupsMetadata().size() == 1) {
        // For the case when group scan has single row group and it was filtered,
        // no need to create new group scan with the same row group.
        return null;
      }
      logger.debug("All row groups have been filtered out. Add back one to get schema from scanner");

      Map<Path, FileMetadata> filesMap = getNextOrEmpty(getFilesMetadata().values()).stream()
          .collect(Collectors.toMap(FileMetadata::getLocation, Function.identity()));

      Multimap<Path, RowGroupMetadata> rowGroupsMap = LinkedListMultimap.create();
      getNextOrEmpty(getRowGroupsMetadata().values()).forEach(entry -> rowGroupsMap.put(entry.getLocation(), entry));

      builder.withRowGroups(rowGroupsMap)
          .withTable(getTableMetadata())
          .withPartitions(getNextOrEmpty(getPartitionsMetadata()))
          .withFiles(filesMap)
          .withMatching(false);
    }

    if (builder.getOverflowLevel() != MetadataLevel.NONE) {
      logger.warn("applyFilter {} wasn't able to do pruning for  all metadata levels filter condition, since metadata count for " +
            "{} level exceeds `planner.store.parquet.rowgroup.filter.pushdown.threshold` value.\n" +
            "But underlying metadata was pruned without filter expression according to the metadata with above level.",
          ExpressionStringBuilder.toString(filterExpr), builder.getOverflowLevel());
    }

    logger.debug("applyFilter {} reduce row groups # from {} to {}",
        ExpressionStringBuilder.toString(filterExpr), getRowGroupsMetadata().size(), builder.getRowGroups().size());

    return builder.build();
  }

  @Override
  protected TupleMetadata getColumnMetadata() {
    TupleMetadata columnMetadata = super.getColumnMetadata();
    if (columnMetadata == null && !getRowGroupsMetadata().isEmpty()) {
      return getRowGroupsMetadata().values().iterator().next().getSchema();
    }
    return columnMetadata;
  }

  // narrows the return type
  protected abstract RowGroupScanFilterer getFilterer();

  protected Multimap<Path, RowGroupMetadata> pruneRowGroupsForFiles(Map<Path, FileMetadata> filteredFileMetadata) {
    Multimap<Path, RowGroupMetadata> prunedRowGroups = LinkedListMultimap.create();
    for (Map.Entry<Path, FileMetadata> filteredPartition : filteredFileMetadata.entrySet()) {
      Multimap<Path, RowGroupMetadata> rowGroupsMetadata = getRowGroupsMetadata();
      Collection<RowGroupMetadata> filesRowGroupMetadata = rowGroupsMetadata.get(filteredPartition.getKey());
      if (CollectionUtils.isNotEmpty(filesRowGroupMetadata)) {
        prunedRowGroups.putAll(filteredPartition.getKey(), filesRowGroupMetadata);
      }
    }

    return prunedRowGroups;
  }

  // filter push down methods block end

  // limit push down methods start
  @Override
  public GroupScan applyLimit(int maxRecords) {
    maxRecords = Math.max(maxRecords, 1); // Make sure it request at least 1 row -> 1 rowGroup.
    if (getTableMetadata() != null) {
      long tableRowCount = (long) TableStatisticsKind.ROW_COUNT.getValue(getTableMetadata());
      if (tableRowCount == NO_COLUMN_STATS || tableRowCount <= maxRecords) {
        logger.debug("limit push down does not apply, since total number of rows [{}] is less or equal to the required [{}].",
            tableRowCount, maxRecords);
        return null;
      }
    }

    List<RowGroupMetadata> qualifiedRowGroups = limitMetadata(getRowGroupsMetadata().values(), maxRecords);

    if (qualifiedRowGroups == null || getRowGroupsMetadata().size() == qualifiedRowGroups.size()) {
      logger.debug("limit push down does not apply, since number of row groups was not reduced.");
      return null;
    }

    Map<Path, FileMetadata> qualifiedFiles;
    Map<Path, FileMetadata> filesMetadata = getFilesMetadata();
    qualifiedFiles = qualifiedRowGroups.stream()
        .map(rowGroup -> filesMetadata.get(rowGroup.getLocation()))
        .filter(Objects::nonNull)
        .collect(Collectors.toMap(FileMetadata::getLocation, Function.identity()));

    Multimap<Path, RowGroupMetadata> prunedRowGroups = LinkedListMultimap.create();

    for (RowGroupMetadata qualifiedRowGroup : qualifiedRowGroups) {
      prunedRowGroups.put(qualifiedRowGroup.getLocation(), qualifiedRowGroup);
    }

    return getFilterer()
        .withRowGroups(prunedRowGroups)
        .withTable(getTableMetadata())
        .withPartitions(getPartitionsMetadata())
        .withFiles(qualifiedFiles)
        .withMatching(matchAllMetadata)
        .build();
  }
  // limit push down methods end

  // helper method used for partition pruning and filter push down
  @Override
  public void modifyFileSelection(FileSelection selection) {
    super.modifyFileSelection(selection);

    List<Path> files = selection.getFiles();
    fileSet = new HashSet<>(files);
    entries = new ArrayList<>(files.size());

    entries.addAll(files.stream()
        .map(ReadEntryWithPath::new)
        .collect(Collectors.toList()));

    Multimap<Path, RowGroupMetadata> newRowGroups = LinkedListMultimap.create();
    if (!getRowGroupsMetadata().isEmpty()) {
      for (Map.Entry<Path, RowGroupMetadata> entry : getRowGroupsMetadata().entries()) {
        if (fileSet.contains(entry.getKey())) {
          newRowGroups.put(entry.getKey(), entry.getValue());
        }
      }
    }
    this.rowGroups = newRowGroups;

    tableMetadata = ParquetTableMetadataUtils.updateRowCount(getTableMetadata(), getRowGroupsMetadata().values());

    if (!getFilesMetadata().isEmpty()) {
      this.files = getFilesMetadata().entrySet().stream()
          .filter(entry -> fileSet.contains(entry.getKey()))
          .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    } else {
      this.files = Collections.emptyMap();
    }

    List<PartitionMetadata> newPartitions = new ArrayList<>();
    if (!getPartitionsMetadata().isEmpty()) {
      for (PartitionMetadata entry : getPartitionsMetadata()) {
        for (Path partLocation : entry.getLocations()) {
          if (fileSet.contains(partLocation)) {
            newPartitions.add(entry);
            break;
          }
        }
      }
    }
    partitions = newPartitions;

    rowGroupInfos = null;
  }

  // protected methods block
  @Override
  protected void init() throws IOException {
    super.init();

    this.partitionColumns = metadataProvider.getPartitionColumns();
    this.endpointAffinities = AffinityCreator.getAffinityMap(getRowGroupInfos());
  }

  protected Multimap<Path, RowGroupMetadata> getRowGroupsMetadata() {
    if (rowGroups == null) {
      rowGroups = ((ParquetMetadataProvider) metadataProvider).getRowGroupsMetadataMap();
    }
    return rowGroups;
  }

  /**
   * Removes metadata which does not belong to any of partitions in metadata list.
   *
   * @param metadataToPrune           list of metadata which should be pruned
   * @param filteredPartitionMetadata list of partition metadata which was pruned
   * @param <T>                       type of metadata to filter
   * @return list with metadata which belongs to pruned partitions
   */
  protected static <T extends BaseMetadata & LocationProvider> Multimap<Path, T> pruneForPartitions(Multimap<Path, T> metadataToPrune,
      List<PartitionMetadata> filteredPartitionMetadata) {
    Multimap<Path, T> prunedFiles = LinkedListMultimap.create();
    if (metadataToPrune != null) {
      for (Map.Entry<Path, T> entry : metadataToPrune.entries()) {
        for (PartitionMetadata filteredPartition : filteredPartitionMetadata) {
          if (filteredPartition.getLocations().contains(entry.getKey())) {
            prunedFiles.put(entry.getKey(), entry.getValue());
            break;
          }
        }
      }
    }

    return prunedFiles;
  }

  // abstract methods block start
  protected abstract Collection<CoordinationProtos.DrillbitEndpoint> getDrillbits();
  protected abstract AbstractParquetGroupScan cloneWithFileSelection(Collection<Path> filePaths) throws IOException;
  // abstract methods block end

  /**
   * This class is responsible for filtering different metadata levels including row group level.
   */
  protected abstract static class RowGroupScanFilterer extends GroupScanWithMetadataFilterer {
    protected Multimap<Path, RowGroupMetadata> rowGroups = LinkedListMultimap.create();

    public RowGroupScanFilterer(AbstractGroupScanWithMetadata source) {
      super(source);
    }

    public RowGroupScanFilterer withRowGroups(Multimap<Path, RowGroupMetadata> rowGroups) {
      this.rowGroups = rowGroups;
      return this;
    }

    /**
     * Returns new {@link AbstractParquetGroupScan} instance to be populated with filtered metadata
     * from this {@link RowGroupScanFilterer} instance.
     *
     * @return new {@link AbstractParquetGroupScan} instance
     */
    protected abstract AbstractParquetGroupScan getNewScan();

    public Multimap<Path, RowGroupMetadata> getRowGroups() {
      return rowGroups;
    }

    @Override
    public AbstractParquetGroupScan build() {
      AbstractParquetGroupScan newScan = getNewScan();
      newScan.tableMetadata = tableMetadata;
      // updates common row count and nulls counts for every column
      if (newScan.getTableMetadata() != null && rowGroups != null && newScan.getRowGroupsMetadata().size() != rowGroups.size()) {
        newScan.tableMetadata = ParquetTableMetadataUtils.updateRowCount(newScan.getTableMetadata(), rowGroups.values());
      }
      newScan.partitions = partitions;
      newScan.files = files;
      newScan.rowGroups = rowGroups;
      newScan.matchAllMetadata = matchAllMetadata;
      // since builder is used when pruning happens, entries and fileSet should be expanded
      if (!newScan.getFilesMetadata().isEmpty()) {
        newScan.entries = newScan.getFilesMetadata().keySet().stream()
            .map(ReadEntryWithPath::new)
            .collect(Collectors.toList());

        newScan.fileSet = new HashSet<>(newScan.getFilesMetadata().keySet());
      } else if (!newScan.getRowGroupsMetadata().isEmpty()) {
        newScan.entries = newScan.getRowGroupsMetadata().keySet().stream()
            .map(ReadEntryWithPath::new)
            .collect(Collectors.toList());

        newScan.fileSet = new HashSet<>(newScan.getRowGroupsMetadata().keySet());
      }

      newScan.endpointAffinities = AffinityCreator.getAffinityMap(newScan.getRowGroupInfos());

      return newScan;
    }

    @Override
    protected RowGroupScanFilterer getFiltered(OptionManager optionManager, FilterPredicate filterPredicate, Set<SchemaPath> schemaPathsInExpr) {
      super.getFiltered(optionManager, filterPredicate, schemaPathsInExpr);

      if (!((AbstractParquetGroupScan) source).getRowGroupsMetadata().isEmpty()) {
        filterRowGroupMetadata(optionManager, filterPredicate, schemaPathsInExpr);
      }
      return this;
    }

    /**
     * Produces filtering of metadata at row group level.
     *
     * @param optionManager     option manager
     * @param filterPredicate   filter expression
     * @param schemaPathsInExpr columns used in filter expression
     */
    protected void filterRowGroupMetadata(OptionManager optionManager,
                                          FilterPredicate filterPredicate,
                                          Set<SchemaPath> schemaPathsInExpr) {
      AbstractParquetGroupScan abstractParquetGroupScan = (AbstractParquetGroupScan) source;
      Multimap<Path, RowGroupMetadata> prunedRowGroups;
      if (!abstractParquetGroupScan.getFilesMetadata().isEmpty()
          && abstractParquetGroupScan.getFilesMetadata().size() > getFiles().size()) {
        // prunes row groups to leave only row groups which are contained by pruned files
        prunedRowGroups = abstractParquetGroupScan.pruneRowGroupsForFiles(getFiles());
      } else if (!abstractParquetGroupScan.getPartitionsMetadata().isEmpty()
          && abstractParquetGroupScan.getPartitionsMetadata().size() > getPartitions().size()) {
        // prunes row groups to leave only row groups which are contained by pruned partitions
        prunedRowGroups = pruneForPartitions(abstractParquetGroupScan.getRowGroupsMetadata(), getPartitions());
      } else {
        // no partition or file pruning happened, no need to prune initial row groups list
        prunedRowGroups = abstractParquetGroupScan.getRowGroupsMetadata();
      }

      if (isMatchAllMetadata()) {
        this.rowGroups = prunedRowGroups;
        return;
      }

      // Stop files pruning for the case:
      //    -  # of row groups is beyond PARQUET_ROWGROUP_FILTER_PUSHDOWN_PLANNING_THRESHOLD.
      if (prunedRowGroups.size() <= optionManager.getOption(
        PlannerSettings.PARQUET_ROWGROUP_FILTER_PUSHDOWN_PLANNING_THRESHOLD)) {
        matchAllMetadata = true;
        List<RowGroupMetadata> filteredRowGroups = filterAndGetMetadata(schemaPathsInExpr, prunedRowGroups.values(), filterPredicate, optionManager);

        this.rowGroups = LinkedListMultimap.create();
        filteredRowGroups.forEach(entry -> this.rowGroups.put(entry.getLocation(), entry));
      } else {
        this.rowGroups = prunedRowGroups;
        matchAllMetadata = false;
        overflowLevel = MetadataLevel.ROW_GROUP;
      }
    }
  }

}
